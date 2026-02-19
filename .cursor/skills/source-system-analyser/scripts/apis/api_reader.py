#!/usr/bin/env python3
"""
Read REST APIs and object/blob URLs: fetch JSON paths, discover endpoints, or download files.
Usage:
  api_reader.py <base_url> [--path PATH ...] [--discover] [--file-url URL ...]
                [--azure-blob-url URL ...] [--azure-account NAME --azure-container NAME --azure-blob PATH]
                [--azure-sas-token TOKEN] [--s3-uri s3://bucket/key ...] [--gcs-uri gs://bucket/key ...]
                [--bearer TOKEN] [--bearer-env NAME] [--bearer-from-keyvault] [--bearer-secret NAME]
                [--api-key KEY] [--header NAME:VALUE]
                [--download FILE] [--download-dir DIR] [--output FILE]
"""

import argparse
import json
import os
import sys
from typing import List, Optional
from urllib.parse import quote, unquote, urljoin, urlparse

import requests

# Load .env from cwd or workspace so KEYVAULT_NAME is available when using --bearer-from-keyvault
def _load_dotenv() -> None:
    for d in [os.getcwd(), CURSOR_ROOT]:
        if not d:
            continue
        env_path = os.path.join(d, ".env")
        if os.path.isfile(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            name, _, value = line.partition("=")
                            name, value = name.strip(), value.strip().strip("'\"")
                            if name and name not in os.environ:
                                os.environ[name] = value
            except OSError:
                pass
            break

SKILL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CURSOR_ROOT = os.path.normpath(os.path.join(SKILL_DIR, "..", ".."))
DEFAULT_FLAT_DIR = os.path.join(SKILL_DIR, "flat")


def _get_keyvault_secret(vault_name: str, secret_name: str) -> Optional[str]:
    """Retrieve a secret value from Azure Key Vault. Returns None on error. Never logs the value."""
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        credential = DefaultAzureCredential()
        kv_url = f"https://{vault_name}.vault.azure.net"
        secret_client = SecretClient(vault_url=kv_url, credential=credential)
        secret = secret_client.get_secret(secret_name)
        return secret.value if secret else None
    except Exception:
        return None

DISCOVERY_PATHS = [
    "/api/tables",
    "/api",
    "/openapi.json",
    "/swagger.json",
    "/docs",
    "/",
]


def main() -> int:
    p = argparse.ArgumentParser(description="Read REST API paths and object/blob file URLs.")
    p.add_argument("base_url", help="Base URL of the API (e.g. http://localhost:8000)")
    p.add_argument("--path", action="append", dest="paths", metavar="PATH", help="Path to fetch (e.g. /api/tables); can repeat")
    p.add_argument("--discover", action="store_true", help="Try common discovery paths")
    p.add_argument(
        "--file-url",
        action="append",
        dest="file_urls",
        metavar="URL",
        help="Full file/object URL to fetch or download (e.g. signed blob URL); can repeat",
    )
    p.add_argument(
        "--azure-blob-url",
        action="append",
        default=[],
        metavar="URL",
        help="Azure blob URL; pairs well with --azure-sas-token; can repeat",
    )
    p.add_argument("--azure-account", metavar="NAME", help="Azure storage account name")
    p.add_argument("--azure-container", metavar="NAME", help="Azure blob container name")
    p.add_argument("--azure-blob", metavar="PATH", help="Azure blob path in container")
    p.add_argument("--azure-sas-token", metavar="TOKEN", help="Azure SAS token (with or without leading '?')")
    p.add_argument(
        "--azure-key-vault",
        metavar="VAULT",
        help="Key Vault name to read storage account key from (use with --azure-account/container/blob, no SAS)",
    )
    p.add_argument(
        "--azure-key-vault-secret",
        metavar="SECRET",
        help="Key Vault secret name for storage key (no default; provide explicitly or via AZURE_STORAGE_KEY_SECRET)",
    )
    p.add_argument(
        "--azure-endpoint-suffix",
        default="blob.core.windows.net",
        metavar="SUFFIX",
        help="Azure blob endpoint suffix (default: blob.core.windows.net)",
    )
    p.add_argument(
        "--s3-uri",
        action="append",
        default=[],
        metavar="s3://bucket/key",
        help="S3 object URI; converted to HTTPS URL; can repeat",
    )
    p.add_argument(
        "--gcs-uri",
        action="append",
        default=[],
        metavar="gs://bucket/key",
        help="GCS object URI; converted to HTTPS URL; can repeat",
    )
    p.add_argument(
        "--flat-file",
        action="append",
        default=[],
        metavar="REL_PATH",
        help="Read local file from flat folder using relative path; can repeat",
    )
    p.add_argument(
        "--flat-dir",
        default=DEFAULT_FLAT_DIR,
        metavar="DIR",
        help="General-purpose local folder for downloaded/read files (default: .cursor/skills/source-system-analyser/flat)",
    )
    p.add_argument("--bearer", metavar="TOKEN", help="Bearer token for Authorization header")
    p.add_argument(
        "--bearer-env",
        metavar="NAME",
        help="Env var name for bearer token lookup (no default; provide explicitly)",
    )
    p.add_argument(
        "--bearer-from-keyvault",
        action="store_true",
        help="Load bearer token from Azure Key Vault (use KEYVAULT_NAME or --key-vault)",
    )
    p.add_argument(
        "--key-vault",
        metavar="VAULT",
        help="Key Vault name for bearer token (overrides KEYVAULT_NAME from .env)",
    )
    p.add_argument(
        "--bearer-secret",
        metavar="NAME",
        help="Key Vault secret name for bearer token (no default; provide explicitly)",
    )
    p.add_argument("--api-key", metavar="KEY", help="API key value")
    p.add_argument("--api-key-header", default="X-API-Key", help="Header name for API key (default: X-API-Key)")
    p.add_argument("--header", action="append", default=[], metavar="NAME:VALUE", help="Additional request header; can repeat")
    p.add_argument("--download", metavar="FILE", help="Download single response body to this file path")
    p.add_argument("--download-dir", metavar="DIR", help="Directory for downloaded files (for multiple URLs/paths)")
    p.add_argument("--output", "-o", metavar="FILE", help="Write JSON to file (default: stdout)")
    p.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds (default: 30)")
    args = p.parse_args()

    _load_dotenv()
    bearer_token = None
    auth_source = "none"
    auth_details = {
        "bearer_env_name": args.bearer_env,
        "bearer_secret_name": args.bearer_secret,
        "key_vault_name": args.key_vault or os.environ.get("KEYVAULT_NAME"),
        "used_default_bearer_env": False,
        "used_default_bearer_secret": False,
    }
    if args.bearer:
        bearer_token = args.bearer
        auth_source = "cli_bearer"
    elif args.bearer_env:
        bearer_token = os.environ.get(args.bearer_env)
        if bearer_token:
            auth_source = "env"
    elif args.bearer_from_keyvault or args.key_vault or os.environ.get("KEYVAULT_NAME"):
        vault_name = args.key_vault or os.environ.get("KEYVAULT_NAME")
        if not args.bearer_secret:
            return _write(
                {
                    "error": "Missing bearer secret name. Provide --bearer-secret <SECRET_NAME>.",
                    "auth_resolution": auth_details,
                },
                args.output,
            )
        if vault_name:
            bearer_token = _get_keyvault_secret(vault_name, args.bearer_secret)
            if bearer_token:
                auth_source = "keyvault"

    base = args.base_url.rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = "http://" + base

    headers = {}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    if args.api_key:
        headers[args.api_key_header] = args.api_key
    for raw in args.header:
        if ":" not in raw:
            return _write({"error": f"Invalid --header value '{raw}'. Expected NAME:VALUE."}, args.output)
        name, value = raw.split(":", 1)
        headers[name.strip()] = value.strip()

    session = requests.Session()
    session.headers.update(headers)
    timeout = args.timeout

    os.makedirs(args.flat_dir, exist_ok=True)

    result = {
        "base_url": base,
        "flat_dir": args.flat_dir,
        "auth_resolution": {"bearer_source": auth_source, **auth_details},
    }
    paths_to_fetch = list(args.paths or [])
    file_urls = list(args.file_urls or [])
    flat_files = list(args.flat_file or [])
    kv_name = args.azure_key_vault or os.environ.get("AZURE_KEY_VAULT_NAME")
    az_account = args.azure_account or os.environ.get("AZURE_STORAGE_ACCOUNT")
    az_container = args.azure_container or os.environ.get("AZURE_STORAGE_CONTAINER")
    az_blob = args.azure_blob or os.environ.get("AZURE_STORAGE_BLOB")
    azure_kv_results = []
    if kv_name and az_account and az_container and az_blob and not args.azure_sas_token:
        secret_name = os.environ.get("AZURE_STORAGE_KEY_SECRET") or args.azure_key_vault_secret
        if not secret_name:
            return _write(
                {
                    "error": "Missing Azure storage key secret name. Provide --azure-key-vault-secret <SECRET_NAME> or AZURE_STORAGE_KEY_SECRET.",
                },
                args.output,
            )
        kv_entry = _fetch_azure_blob_via_keyvault(
            kv_name,
            secret_name,
            az_account,
            az_container,
            az_blob,
            args.azure_endpoint_suffix,
            _target_destination(
                args.download,
                args.download_dir,
                f"https://{az_account}.{args.azure_endpoint_suffix}/{az_container}/{az_blob}",
                True,
                args.flat_dir,
            ),
        )
        if kv_entry:
            azure_kv_results.append(kv_entry)
    else:
        file_urls.extend(_build_azure_urls(args))
    file_urls.extend(_convert_object_uris(args.s3_uri, "s3"))
    file_urls.extend(_convert_object_uris(args.gcs_uri, "gcs"))

    total_file_targets = len(paths_to_fetch) + len(file_urls) + len(azure_kv_results)
    if args.download and total_file_targets != 1:
        return _write(
            {"error": "--download requires exactly one --path, --file-url, or Azure blob target."},
            args.output,
        )

    if args.discover:
        discovery = []
        for path in DISCOVERY_PATHS:
            url = urljoin(base + "/", path.lstrip("/"))
            try:
                r = session.get(url, timeout=timeout)
                try:
                    data = r.json() if r.content else None
                except Exception:
                    data = r.text[:2000] if r.text else None
                discovery.append({"path": path, "status_code": r.status_code, "data": data})
                if path == "/api/tables" and r.status_code == 200 and isinstance(data, dict) and "tables" in data:
                    result["tables"] = data.get("tables", [])
            except requests.RequestException as e:
                discovery.append({"path": path, "error": str(e)})
        result["discovery"] = discovery
        if not paths_to_fetch:
            return _write(result, args.output)

    if not paths_to_fetch and not file_urls and not azure_kv_results and not flat_files:
        return _write(result, args.output)

    path_results = []
    for path in paths_to_fetch:
        url = urljoin(base + "/", path.lstrip("/"))
        destination = _target_destination(
            args.download,
            args.download_dir,
            url,
            len(paths_to_fetch) + len(file_urls) == 1,
            args.flat_dir,
        )
        entry = _fetch_url(session, url, timeout, destination=destination)
        entry["path"] = path
        path_results.append(entry)

    if len(path_results) == 1:
        result.update(path_results[0])
    elif path_results:
        result["paths"] = path_results

    file_results = []
    for file_url in file_urls:
        destination = _target_destination(
            args.download,
            args.download_dir,
            file_url,
            len(paths_to_fetch) + len(file_urls) == 1,
            args.flat_dir,
        )
        entry = _fetch_url(session, file_url, timeout, destination=destination)
        entry["url"] = file_url
        file_results.append(entry)

    all_file_results = azure_kv_results + file_results
    if len(all_file_results) == 1:
        result["file"] = all_file_results[0]
    elif all_file_results:
        result["files"] = all_file_results

    local_results = []
    for rel_path in flat_files:
        local_results.append(_read_flat_file(args.flat_dir, rel_path))

    if len(local_results) == 1:
        result["flat_file"] = local_results[0]
    elif local_results:
        result["flat_files"] = local_results

    return _write(result, args.output)


def _write(obj: dict, output: Optional[str]) -> int:
    out = json.dumps(obj, indent=2, default=str)
    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(out)
        return 0
    print(out)
    return 0


def _target_destination(
    download_file: Optional[str], download_dir: Optional[str], url: str, single_target: bool, flat_dir: str
) -> Optional[str]:
    if download_file and single_target:
        if os.path.isabs(download_file):
            return download_file
        os.makedirs(flat_dir, exist_ok=True)
        destination = os.path.join(flat_dir, download_file)
        parent = os.path.dirname(destination)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return destination
    selected_dir = download_dir or flat_dir
    if selected_dir:
        os.makedirs(selected_dir, exist_ok=True)
        return os.path.join(selected_dir, _filename_from_url(url))
    return None


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = unquote(parsed.path.rstrip("/").split("/")[-1]) if parsed.path else ""
    return name or "download.bin"


def _fetch_url(session: requests.Session, url: str, timeout: int, destination: Optional[str] = None) -> dict:
    try:
        r = session.get(url, timeout=timeout, stream=bool(destination))
        entry = {"status_code": r.status_code}
        if destination:
            with open(destination, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)
            entry["downloaded_to"] = destination
            entry["content_type"] = r.headers.get("Content-Type")
            entry["content_length"] = r.headers.get("Content-Length")
            return entry
        try:
            entry["data"] = r.json() if r.content else None
        except Exception:
            entry["data"] = r.text[:5000] if r.text else None
        return entry
    except requests.RequestException as e:
        return {"error": str(e)}


def _fetch_azure_blob_via_keyvault(
    vault_name: str,
    secret_name: str,
    account: str,
    container: str,
    blob_path: str,
    endpoint_suffix: str,
    destination: Optional[str],
) -> Optional[dict]:
    """Fetch blob using storage account key from Key Vault. Returns result dict or None on error."""
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
        from azure.storage.blob import BlobServiceClient

        credential = DefaultAzureCredential()
        kv_url = f"https://{vault_name}.vault.azure.net"
        secret_client = SecretClient(vault_url=kv_url, credential=credential)
        key = secret_client.get_secret(secret_name).value
        ep = "core.windows.net" if "blob.core.windows.net" in endpoint_suffix else endpoint_suffix
        conn = f"DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key};EndpointSuffix={ep}"
        client = BlobServiceClient.from_connection_string(conn)
        blob_client = client.get_container_client(container).get_blob_client(blob_path)
        content = blob_client.download_blob().readall()
        entry = {"url": f"https://{account}.{endpoint_suffix}/{container}/{blob_path}", "status_code": 200}
        if destination:
            with open(destination, "wb") as f:
                f.write(content)
            entry["downloaded_to"] = destination
            entry["content_type"] = blob_client.get_blob_properties().content_settings.content_type
            entry["content_length"] = len(content)
        else:
            try:
                text = content.decode("utf-8")
                try:
                    entry["data"] = json.loads(text)
                except Exception:
                    entry["text_preview"] = text[:5000]
            except UnicodeDecodeError:
                entry["binary"] = True
                entry["preview_hex"] = content[:120].hex()
        return entry
    except Exception as e:
        return {"url": f"https://{account}.{endpoint_suffix}/{container}/{blob_path}", "error": str(e)}


def _build_azure_urls(args: argparse.Namespace) -> List[str]:
    urls = []

    for u in args.azure_blob_url:
        urls.append(_append_query_token(u, args.azure_sas_token))

    if any([args.azure_account, args.azure_container, args.azure_blob]):
        if not all([args.azure_account, args.azure_container, args.azure_blob]):
            raise ValueError(
                "Azure URL components require --azure-account, --azure-container, and --azure-blob together."
            )
        blob_path = quote(args.azure_blob.lstrip("/"), safe="/")
        base_url = (
            f"https://{args.azure_account}.{args.azure_endpoint_suffix}/"
            f"{args.azure_container}/{blob_path}"
        )
        urls.append(_append_query_token(base_url, args.azure_sas_token))

    return urls


def _append_query_token(url: str, token: Optional[str]) -> str:
    if not token:
        return url
    clean = token[1:] if token.startswith("?") else token
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{clean}"


def _convert_object_uris(uris: List[str], provider: str) -> List[str]:
    out = []
    for uri in uris:
        parsed = urlparse(uri)
        if provider == "s3" and parsed.scheme != "s3":
            raise ValueError(f"Invalid S3 URI '{uri}'. Expected s3://bucket/key.")
        if provider == "gcs" and parsed.scheme != "gs":
            raise ValueError(f"Invalid GCS URI '{uri}'. Expected gs://bucket/key.")
        if not parsed.netloc or not parsed.path or parsed.path == "/":
            raise ValueError(f"Invalid object URI '{uri}'. Missing bucket or key.")
        bucket = parsed.netloc
        key = quote(parsed.path.lstrip("/"), safe="/")
        if provider == "s3":
            out.append(f"https://{bucket}.s3.amazonaws.com/{key}")
        else:
            out.append(f"https://storage.googleapis.com/{bucket}/{key}")
    return out


def _read_flat_file(flat_dir: str, rel_path: str) -> dict:
    full_path = _safe_join(flat_dir, rel_path)
    if not os.path.exists(full_path):
        return {"path": rel_path, "error": "File not found in flat folder"}
    if not os.path.isfile(full_path):
        return {"path": rel_path, "error": "Path is not a file"}

    size = os.path.getsize(full_path)
    entry = {"path": rel_path, "full_path": full_path, "size_bytes": size}
    try:
        with open(full_path, "rb") as f:
            raw = f.read()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            entry["binary"] = True
            entry["preview_hex"] = raw[:120].hex()
            return entry
        try:
            entry["data"] = json.loads(text)
        except Exception:
            entry["text_preview"] = text[:5000]
        return entry
    except OSError as e:
        return {"path": rel_path, "error": str(e)}


def _safe_join(base_dir: str, rel_path: str) -> str:
    candidate = os.path.abspath(os.path.join(base_dir, rel_path))
    base_abs = os.path.abspath(base_dir)
    if os.path.commonpath([candidate, base_abs]) != base_abs:
        raise ValueError(f"Invalid flat file path '{rel_path}'. Must stay within flat folder.")
    return candidate


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ValueError as e:
        print(json.dumps({"error": str(e)}, indent=2))
        sys.exit(2)
