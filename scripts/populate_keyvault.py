#!/usr/bin/env python3
"""
Populate Azure Key Vault with secrets from .env.
Usage:
  python scripts/populate_keyvault.py [--vault VAULT_NAME] [--user USER_NAME]

  --vault: Key Vault name (default: KEYVAULT_NAME from .env)
  --user:  Optional; create KEY-USER secrets for per-user overrides
"""
import argparse
import os
import sys
from pathlib import Path

# Load .env for KEYVAULT_NAME
try:
    from dotenv import load_dotenv
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
        env_path = base / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass

# Skip these keys - config, not secrets
SKIP_KEYS = {"KEYVAULT_NAME", "AZURE_USER_NAME"}


def _env_to_secret_name(env_key: str) -> str:
    """Convert env var name to Key Vault secret name (underscores -> hyphens)."""
    return env_key.replace("_", "-")


def main():
    parser = argparse.ArgumentParser(description="Populate Key Vault from .env")
    parser.add_argument("--vault", default=os.environ.get("KEYVAULT_NAME"), help="Key Vault name")
    parser.add_argument("--user", help="Create KEY-USER secrets for per-user overrides")
    args = parser.parse_args()

    if not args.vault:
        print("ERROR: Set KEYVAULT_NAME in .env or pass --vault", file=sys.stderr)
        sys.exit(1)

    env_path = Path.cwd() / ".env"
    if not env_path.exists():
        env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        print("ERROR: .env not found", file=sys.stderr)
        sys.exit(1)

    vars_to_set = []
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key in SKIP_KEYS or not value:
                continue
            vars_to_set.append((key, value))

    if not vars_to_set:
        print("No secrets to upload from .env")
        sys.exit(0)

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        print("ERROR: Install azure-identity and azure-keyvault-secrets", file=sys.stderr)
        sys.exit(1)

    url = f"https://{args.vault}.vault.azure.net/"
    client = SecretClient(vault_url=url, credential=DefaultAzureCredential())

    suffix = f"-{args.user.upper()}" if args.user else ""
    for key, value in vars_to_set:
        secret_name = f"{_env_to_secret_name(key)}{suffix}"
        try:
            client.set_secret(secret_name, value)
            print(f"  {secret_name}")
        except Exception as e:
            print(f"  {secret_name}: FAILED - {e}", file=sys.stderr)

    print(f"Done. Uploaded {len(vars_to_set)} secret(s) to {args.vault}")


if __name__ == "__main__":
    main()
