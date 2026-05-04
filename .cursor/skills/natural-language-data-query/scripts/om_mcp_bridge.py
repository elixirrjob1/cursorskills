#!/usr/bin/env python3
"""
OpenMetadata MCP stdio Bridge
==============================
Sits between Cursor's MCP client (stdio) and OpenMetadata's native HTTP MCP
endpoint ({OM_URL}/mcp).  Handles authentication automatically so Cursor does
not need to manage tokens.

Credential resolution order (highest priority first):
  1. Cloud secrets manager (SECRETS_PROVIDER env var)     → production
  2. OPENMETADATA_TOKEN env var                           → pre-generated JWT
  3. OPENMETADATA_API_URL + USERNAME + PASSWORD env vars  → auto-login (.env)

All credentials should be stored in the workspace .env file (gitignored) or
in a cloud secrets manager for production. Do not hardcode credentials here.

Transport:
  Cursor ──stdio/NDJSON──► this bridge ──HTTP POST──► {OM_URL}/mcp
"""

import base64
import json
import logging
import os
import sys
from typing import Optional

import requests

# Auto-load .env from the workspace root (two levels up from this script)
try:
    from dotenv import load_dotenv, find_dotenv
    _dotenv_path = find_dotenv(usecwd=False)
    if _dotenv_path:
        load_dotenv(_dotenv_path, override=False)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Logging — write to stderr so it doesn't pollute the MCP stdout channel
# ---------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="[om-bridge] %(levelname)s %(message)s",
)
log = logging.getLogger("om-bridge")

# ---------------------------------------------------------------------------
# Cloud secrets manager support (production path)
# ---------------------------------------------------------------------------

_OM_SECRET_KEYS = {
    "om_host":     "openmetadata-api-url",
    "om_username": "openmetadata-username",
    "om_password": "openmetadata-password",
    "om_token":    "openmetadata-token",
}


def _from_azure_keyvault(vault_url: str) -> dict:
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        raise RuntimeError("Install azure-keyvault-secrets azure-identity for Azure KV support")
    cred = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=cred)
    result = {}
    for key, secret_name in _OM_SECRET_KEYS.items():
        try:
            result[key] = client.get_secret(secret_name).value or ""
        except Exception:
            result[key] = ""
    return result


def _from_aws_secrets(region: str, secret_name: str) -> dict:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("Install boto3 for AWS Secrets Manager support")
    client = boto3.client("secretsmanager", region_name=region)
    raw = client.get_secret_value(SecretId=secret_name)
    data = json.loads(raw.get("SecretString", "{}"))
    return {key: data.get(sname, "") for key, sname in _OM_SECRET_KEYS.items()}


def _from_hashicorp(vault_addr: str, token: str, path: str) -> dict:
    try:
        import hvac
    except ImportError:
        raise RuntimeError("Install hvac for HashiCorp Vault support")
    client = hvac.Client(url=vault_addr, token=token)
    if "/data/" in path:
        mount, _, kv_path = path.partition("/data/")
        data = client.secrets.kv.v2.read_secret_version(path=kv_path, mount_point=mount)["data"]["data"]
    else:
        data = (client.read(path) or {}).get("data", {})
    return {key: data.get(sname, "") for key, sname in _OM_SECRET_KEYS.items()}


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------

def resolve_credentials() -> dict:
    """
    Return dict with keys: om_host, om_username, om_password, om_token.
    Resolution order: secrets manager → env vars (from .env or mcp.json env block).
    """
    provider = os.environ.get("SECRETS_PROVIDER", "").lower()
    secrets_url = os.environ.get("SECRETS_URL", "")

    if provider == "azure-keyvault" and secrets_url:
        log.info("Reading OM credentials from Azure Key Vault: %s", secrets_url)
        return _from_azure_keyvault(secrets_url)
    elif provider == "aws-secrets-manager":
        region = os.environ.get("SECRETS_REGION", "us-east-1")
        name   = os.environ.get("SECRETS_NAME", "businesschat/creds")
        log.info("Reading OM credentials from AWS Secrets Manager: %s / %s", region, name)
        return _from_aws_secrets(region, name)
    elif provider == "hashicorp-vault" and secrets_url:
        token = os.environ.get("VAULT_TOKEN", "")
        path  = os.environ.get("SECRETS_PATH", "secret/data/businesschat/creds")
        log.info("Reading OM credentials from HashiCorp Vault: %s", path)
        return _from_hashicorp(secrets_url, token, path)

    # Env vars — loaded from .env (auto-loaded at startup) or mcp.json env block
    creds: dict = {
        "om_host":     os.environ.get("OPENMETADATA_API_URL") or os.environ.get("OPENMETADATA_HOST", ""),
        "om_username": os.environ.get("OPENMETADATA_USERNAME", ""),
        "om_password": os.environ.get("OPENMETADATA_PASSWORD", ""),
        "om_token":    os.environ.get("OPENMETADATA_TOKEN", ""),
    }

    return creds


# ---------------------------------------------------------------------------
# OpenMetadata JWT login
# ---------------------------------------------------------------------------

def get_jwt(creds: dict) -> str:
    """Return a valid JWT for the OpenMetadata MCP endpoint."""
    if creds.get("om_token"):
        log.info("Using pre-supplied OpenMetadata token")
        return creds["om_token"]

    host = creds.get("om_host", "").rstrip("/")
    username = creds.get("om_username", "")
    password = creds.get("om_password", "")

    if not (host and username and password):
        missing = [k for k in ("om_host", "om_username", "om_password") if not creds.get(k)]
        raise RuntimeError(
            f"OpenMetadata credentials incomplete — missing: {missing}. "
            "Set OPENMETADATA_API_URL / OPENMETADATA_USERNAME / OPENMETADATA_PASSWORD "
            "or add them to reference.md."
        )

    log.info("Logging into OpenMetadata as %s @ %s", username, host)
    b64_pwd = base64.b64encode(password.encode()).decode()
    resp = requests.post(
        f"{host}/api/v1/users/login",
        json={"email": username, "password": b64_pwd},
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"OpenMetadata login failed ({resp.status_code}): {resp.text[:300]}"
        )
    data = resp.json()
    token = data.get("accessToken") or data.get("token") or data.get("access_token", "")
    if not token:
        raise RuntimeError(f"Login response missing token — keys: {list(data.keys())}")
    log.info("OpenMetadata login successful")
    return token


# ---------------------------------------------------------------------------
# MCP proxy loop
# ---------------------------------------------------------------------------

def run_bridge(mcp_url: str, jwt: str) -> None:
    """
    Read JSON-RPC 2.0 messages from stdin (NDJSON), forward each to the
    OpenMetadata MCP HTTP endpoint, write the response to stdout.
    """
    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type":  "application/json",
        "Accept":        "application/json, text/event-stream",
    }

    log.info("MCP bridge ready — proxying to %s", mcp_url)

    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        # Parse incoming JSON-RPC message
        try:
            msg = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            log.warning("Skipping malformed JSON: %s", exc)
            continue

        # MCP notifications (no 'id') — do not expect a response
        if "id" not in msg:
            log.debug("Notification received (method=%s), no response needed", msg.get("method"))
            continue

        msg_id = msg["id"]

        # Forward to OpenMetadata MCP
        try:
            resp = requests.post(mcp_url, headers=headers, json=msg, timeout=60)
            response = resp.json()
        except requests.Timeout:
            response = {
                "jsonrpc": "2.0", "id": msg_id,
                "error": {"code": -32603, "message": "OpenMetadata MCP request timed out"},
            }
        except Exception as exc:
            response = {
                "jsonrpc": "2.0", "id": msg_id,
                "error": {"code": -32603, "message": f"Bridge error: {exc}"},
            }

        # Write response to stdout (MCP client reads from here)
        print(json.dumps(response), flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        creds = resolve_credentials()
    except Exception as exc:
        _fatal(f"Credential resolution failed: {exc}")

    om_host = creds.get("om_host", "").rstrip("/")
    if not om_host:
        _fatal("Cannot determine OpenMetadata host. Set OPENMETADATA_API_URL or add it to reference.md.")

    try:
        jwt = get_jwt(creds)
    except Exception as exc:
        _fatal(f"Authentication failed: {exc}")

    mcp_url = f"{om_host}/mcp"
    run_bridge(mcp_url, jwt)


def _fatal(msg: str) -> None:
    """Print an MCP-formatted error to stdout and exit."""
    log.error(msg)
    print(json.dumps({
        "jsonrpc": "2.0",
        "error":   {"code": -32000, "message": msg},
    }), flush=True)
    sys.exit(1)


if __name__ == "__main__":
    main()
