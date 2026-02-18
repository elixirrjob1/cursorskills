---
name: api-reader
description: Reads REST APIs and object/blob file URLs given a base URL (or full file URL) and optional auth (Bearer/API key/custom headers). Fetches JSON from paths, discovers endpoints (e.g. /api/tables, OpenAPI), and can download files from storage links. Use when the user wants to call APIs, introspect endpoints, fetch JSON, or read/download files from blob/object storage URLs.
---

# API Reader

## Overview

This skill provides a script that reads from REST APIs and file/object URLs: the user supplies a **base URL** (for API paths) and optionally auth headers. The script can:

- **Fetch** one or more paths (e.g. `/api/tables`, `/api/customers`) and return JSON.
- **Discover** common entry points (e.g. `/api/tables`, `/openapi.json`, `/swagger.json`) and report what the API exposes.
- **Read or download files** from full object/blob URLs (e.g. signed URLs from Azure Blob/S3/GCS).
- **Build common storage URLs** from provider-specific inputs (Azure account/container/blob, `s3://`, `gs://`).
- **Read local files** from a general-purpose folder: `.cursor/flat/`.

Output is written as JSON (to a file or stdout) and optional binary downloads are saved to disk.

## Flat Folder (General Purpose)

- Folder: `.cursor/flat/`
- Purpose: shared local drop zone for files the skill downloads and reads.
- Default behavior: when downloading without an explicit absolute path, files are written to this folder.
- Local reads: use `--flat-file REL_PATH` to read files from this folder only.

**When to use:**
- User provides an API URL and wants to read or explore it
- User has an API key or Bearer token and wants to fetch data from an API
- Need to list endpoints or resources (e.g. tables, collections) an API offers
- Need to read or download files from blob/object storage links
- Need to inspect local files previously downloaded by this skill
- Ingesting or documenting a third-party or internal REST API
- Testing or simulating a consumer of the database JSON API (or any similar API)

## Obtaining API URL and Auth

1. **User-provided**: If the user gives a base URL (e.g. `http://localhost:8000` or `https://api.example.com`) and optionally a key or token, use them directly.
2. **Environment variables**: `API_BASE_URL`, `API_URL`, `API_TOKEN`, `API_KEY`, `BEARER_TOKEN` (script does not load .env by default; caller can export or pass via args).
3. **Ask the user**: If the base URL is missing, ask: "Please provide the API base URL (e.g. https://api.example.com). If the API requires authentication, provide a Bearer token or API key."

**Auth options:**
- **Bearer token**: `Authorization: Bearer <token>` — use `--bearer TOKEN`.
- **Azure Key Vault** (for blob with account key): Set `AZURE_KEY_VAULT_NAME`, `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_CONTAINER`, `AZURE_STORAGE_BLOB`; optionally `AZURE_STORAGE_KEY_SECRET`. Requires `az login` or managed identity.
- **API key in header**: e.g. `X-API-Key: <key>` — use `--api-key KEY` (default header `X-API-Key`) or `--api-key KEY --api-key-header "Authorization"` for custom header.
- **Custom headers**: e.g. storage/vendor headers — use `--header "Name:Value"` (repeatable).

**Security**: Never hardcode credentials. Prefer env vars or pass as script arguments; avoid logging tokens.

## Prerequisites

- Python 3.8+
- `requests` — HTTP client (install with `pip install requests`)
- For Azure blob + Key Vault: `azure-identity`, `azure-keyvault-secrets`, `azure-storage-blob` (install with `pip install azure-identity azure-keyvault-secrets azure-storage-blob`)

## Running the Script

From the project root (or ensure `requests` is installed):

```bash
# Install dependency once
.venv/bin/pip install --quiet requests

# Fetch a single path (no auth)
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --path /api/tables --output result.json

# With Bearer token
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --bearer YOUR_TOKEN --path /api/tables --output result.json

# With API key (default header X-API-Key)
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --api-key YOUR_KEY --path /api/customers --output data.json

# Discover common endpoints and list what's available
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --bearer YOUR_TOKEN --discover --output discovery.json

# Multiple paths
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --bearer YOUR_TOKEN --path /api/tables --path /api/customers --output combined.json

# Read from a full blob/object URL (metadata/response capture)
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --file-url "https://storage.example.com/container/file.csv?sig=..." --output file-read.json

# Download a single object URL to a file
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --file-url "https://storage.example.com/container/file.csv?sig=..." --download ./downloads/file.csv --output download-meta.json

# Download multiple object URLs to a directory
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --file-url "https://storage.example.com/a.bin?sig=..." --file-url "https://storage.example.com/b.bin?sig=..." --download-dir ./downloads --output downloads.json

# Azure: build URL from account/container/blob + SAS token
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --azure-account mystorage --azure-container exports --azure-blob "daily/report.csv" --azure-sas-token "?sv=..." --download ./downloads/report.csv --output azure-download.json

# Azure: append SAS to existing blob URL
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --azure-blob-url "https://mystorage.blob.core.windows.net/exports/report.csv" --azure-sas-token "sv=..." --output azure-read.json

# Azure: read blob using storage key from Key Vault (no SAS needed)
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://placeholder --azure-account fmarichub20474397334 --azure-container data --azure-blob csvtest --azure-key-vault skills-fmaric-kv --download csvtest --output blob.json

# S3/GCS URI convenience (converted to HTTPS URL)
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --s3-uri s3://my-bucket/path/to/file.json --gcs-uri gs://my-gcs-bucket/path/data.csv --output objects.json

# Read a local file from the flat folder
.venv/bin/python .cursor/skills/api-reader/scripts/api_reader.py https://api.example.com --flat-file report.csv --output local-file.json
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `base_url` | Yes | Base URL of the API (e.g. `http://localhost:8000`, no trailing slash) |
| `--path` | No* | Path to fetch (e.g. `/api/tables`). Can be repeated. *Required unless `--discover` |
| `--discover` | No | Try common discovery paths and list available endpoints/resources |
| `--file-url` | No | Full file/object URL (blob, signed URL, etc.). Can be repeated |
| `--azure-blob-url` | No | Azure blob URL; can be repeated and combined with `--azure-sas-token` |
| `--azure-account` | No | Azure storage account name (use with `--azure-container` and `--azure-blob`) |
| `--azure-container` | No | Azure blob container name (used with account/blob) |
| `--azure-blob` | No | Azure blob path (used with account/container) |
| `--azure-sas-token` | No | SAS token appended to Azure blob URL (with or without `?`) |
| `--azure-key-vault` | No | Key Vault name to read storage account key from (use with account/container/blob, no SAS). Env: `AZURE_KEY_VAULT_NAME` |
| `--azure-key-vault-secret` | No | Key Vault secret name for storage key. Env: `AZURE_STORAGE_KEY_SECRET` |
| `--azure-endpoint-suffix` | No | Azure endpoint suffix (default: `blob.core.windows.net`) |
| `--s3-uri` | No | S3 URI in form `s3://bucket/key`; converted to HTTPS URL; can be repeated |
| `--gcs-uri` | No | GCS URI in form `gs://bucket/key`; converted to HTTPS URL; can be repeated |
| `--flat-file` | No | Relative file path inside flat folder to read locally; can be repeated |
| `--flat-dir` | No | Override flat folder path (default: `.cursor/flat/`) |
| `--bearer` | No | Bearer token for `Authorization: Bearer <token>` |
| `--api-key` | No | API key value (sent in header, see `--api-key-header`) |
| `--api-key-header` | No | Header name for API key (default: `X-API-Key`) |
| `--header` | No | Extra header in `Name:Value` format. Can be repeated |
| `--download` | No | Save single path/file response body to this file |
| `--download-dir` | No | Save responses to this directory (auto file name from URL/path) |
| `--output` | No | Write result to this JSON file (default: stdout) |
| `--timeout` | No | Request timeout in seconds (default: 30) |

If both `--path` and `--discover` are used, the script runs discovery first, then fetches the given paths. `--download` requires exactly one target (`--path` or storage URL input).
`--s3-uri` and `--gcs-uri` are URL conversion helpers only; they do not generate signed requests.
`--flat-file` only reads files within the flat folder (path traversal is blocked).

## Output Format

- **Single path**: `{"url": "...", "path": "...", "status_code": 200, "data": <response JSON>}`  
- **Multiple paths**: `{"paths": [ {"path": "...", "status_code": 200, "data": ...}, ... ]}`  
- **Discover**: `{"base_url": "...", "discovery": [ {"path": "...", "status_code": 200, "data": ... }, ... ], "tables": ["customers", ...] }` when `/api/tables` returns a list; otherwise discovery entries only.
- **File URL read**: `{"file": {"url": "...", "status_code": 200, "data": ...}}`
- **Download**: includes `downloaded_to`, `content_type`, and `content_length`.
- **Flat file read**: includes `path`, `full_path`, size and either parsed JSON, text preview, or binary preview.

## Script Reference

Implementation: `.cursor/skills/api-reader/scripts/api_reader.py`
