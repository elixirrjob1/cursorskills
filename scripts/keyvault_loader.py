#!/usr/bin/env python3
"""
Load environment variables from Azure Key Vault, with optional per-user overrides.
Falls back to .env when Key Vault is unavailable or not configured.
"""
import os
from pathlib import Path

# Load KEYVAULT_NAME and AZURE_USER_NAME from .env first (before we need them)
try:
    from dotenv import load_dotenv
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
        env_path = base / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass

# Known env vars to fetch from Key Vault (in lookup order for per-user)
ENV_VARS = (
    "DATABASE_URL",
    "GITHUB",
    "MSSQL_URL",
    "SCHEMA",
    "ORACLE_URL",
    "AZURE_MSSQL_URL",
    "AZURE_MSSQL_SCHEMA",
    "API_AUTH_TOKEN",
)


def _env_to_secret_name(env_key: str) -> str:
    """Convert env var name to Key Vault secret name (underscores -> hyphens)."""
    return env_key.replace("_", "-")


def _load_from_dotenv() -> None:
    """Load all vars from .env as fallback."""
    try:
        from dotenv import load_dotenv
        for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
            env_path = base / ".env"
            if env_path.exists():
                load_dotenv(env_path)
                return
    except ImportError:
        pass
    # Manual parse if dotenv not available
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
        env_path = base / ".env"
        if env_path.exists():
            try:
                with open(env_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, _, value = line.partition("=")
                            key = key.strip()
                            value = value.strip().strip('"').strip("'")
                            if key and key not in os.environ:
                                os.environ[key] = value
            except Exception:
                pass
            return


def load_env() -> None:
    """
    Load env vars from Azure Key Vault (or .env fallback).
    - KEYVAULT_NAME: vault name (required for Key Vault)
    - AZURE_USER_NAME: optional; use {VAR}-{USER} secrets first, then {VAR}
    - Does not overwrite existing os.environ values (allows CLI overrides)
    """
    vault_name = os.environ.get("KEYVAULT_NAME", "").strip()
    user_name = os.environ.get("AZURE_USER_NAME", "").strip().upper()

    if not vault_name:
        _load_from_dotenv()
        return

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        _load_from_dotenv()
        return

    try:
        credential = DefaultAzureCredential()
        url = f"https://{vault_name}.vault.azure.net/"
        client = SecretClient(vault_url=url, credential=credential)
    except Exception:
        _load_from_dotenv()
        return

    for var in ENV_VARS:
        if var in os.environ:
            continue  # Do not overwrite (CLI override)
        secret_names = []
        base_name = _env_to_secret_name(var)
        if user_name:
            secret_names.append(f"{base_name}-{user_name}")
        secret_names.append(base_name)
        for name in secret_names:
            try:
                secret = client.get_secret(name)
                if secret and secret.value:
                    os.environ[var] = secret.value
                    break
            except Exception:
                continue

    # If we got nothing from Key Vault, fill gaps from .env
    _load_from_dotenv()
