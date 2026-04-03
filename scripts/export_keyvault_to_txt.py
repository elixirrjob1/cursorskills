#!/usr/bin/env python3
"""
Export all Azure Key Vault secrets to a text file (one KEY=value per line).

Secret names use hyphens in the vault; keys in the file use underscores (same
convention as scripts/keyvault_loader.py).

Requires:
  - KEYVAULT_NAME in the environment (or .env)
  - Azure auth: DefaultAzureCredential (e.g. `az login`, or managed identity)

Install: pip install -r requirements.txt (azure-identity, azure-keyvault-secrets)

The output file may contain highly sensitive values. Do not commit it.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _load_dotenv_minimal() -> None:
    """Load .env from cwd or repo root so KEYVAULT_NAME is set."""
    try:
        from dotenv import load_dotenv

        for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
            p = base / ".env"
            if p.is_file():
                load_dotenv(p)
                return
    except ImportError:
        pass
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
        p = base / ".env"
        if not p.is_file():
            continue
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
        return


def _secret_name_to_key(name: str) -> str:
    return name.replace("-", "_")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path(".cursor/flat/env_export_vault.txt"),
        help="Output file (default: .cursor/flat/env_export_vault.txt)",
    )
    p.add_argument(
        "--vault-name",
        metavar="NAME",
        help="Override KEYVAULT_NAME",
    )
    p.add_argument(
        "--names-only",
        action="store_true",
        help="List secret names only (no values); writes one name per line",
    )
    p.add_argument(
        "--export-shell",
        action="store_true",
        help="Prefix each line with 'export ' (only with values, not --names-only)",
    )
    args = p.parse_args()

    _load_dotenv_minimal()
    vault_name = (args.vault_name or os.environ.get("KEYVAULT_NAME", "")).strip()
    if not vault_name:
        print(
            "error: KEYVAULT_NAME is not set (use .env or --vault-name)",
            file=sys.stderr,
        )
        return 1

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as e:
        print(
            "error: install azure-identity and azure-keyvault-secrets "
            f"(pip install -r requirements.txt): {e}",
            file=sys.stderr,
        )
        return 1

    try:
        credential = DefaultAzureCredential()
        client = SecretClient(
            vault_url=f"https://{vault_name}.vault.azure.net/",
            credential=credential,
        )
    except Exception as e:
        print(f"error: could not create Key Vault client: {e}", file=sys.stderr)
        return 1

    lines: list[str] = []
    prefix = "export " if args.export_shell and not args.names_only else ""

    try:
        secret_props = sorted(
            client.list_properties_of_secrets(),
            key=lambda sp: sp.name,
        )
    except Exception as e:
        print(f"error: could not list secrets: {e}", file=sys.stderr)
        return 1

    for sp in secret_props:
        if not sp.enabled:
            continue
        name = sp.name
        if args.names_only:
            lines.append(name)
            continue
        try:
            secret = client.get_secret(name)
            val = secret.value if secret else ""
            key = _secret_name_to_key(name)
            if val is None:
                val = ""
            if "\n" in val or "\r" in val:
                safe = repr(val)
            else:
                safe = val
            lines.append(f"{prefix}{key}={safe}")
        except Exception as e:
            print(f"warning: could not read secret {name!r}: {e}", file=sys.stderr)

    body = "\n".join(lines)
    if body and not body.endswith("\n"):
        body += "\n"

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(body, encoding="utf-8")
    what = "name(s)" if args.names_only else "line(s)"
    print(f"Wrote {len(lines)} {what} to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
