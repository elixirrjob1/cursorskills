---
name: api-reader
description: Reads any REST API given a base URL and optional API key or Bearer token. Fetches JSON from specified paths or discovers endpoints (e.g. /api/tables, OpenAPI). Use when the user wants to call an API, introspect endpoints, fetch API data as JSON, or provide an API URL and key.
---

# API Reader

## Overview

This skill provides a script that reads from any REST API: the user supplies a **base URL** and optionally an **API key or Bearer token**. The script can:

- **Fetch** one or more paths (e.g. `/api/tables`, `/api/customers`) and return JSON.
- **Discover** common entry points (e.g. `/api/tables`, `/openapi.json`, `/swagger.json`) and report what the API exposes.

Output is written as JSON (to a file or stdout) for use in pipelines or documentation.

**When to use:**
- User provides an API URL and wants to read or explore it
- User has an API key or Bearer token and wants to fetch data from an API
- Need to list endpoints or resources (e.g. tables, collections) an API offers
- Ingesting or documenting a third-party or internal REST API
- Testing or simulating a consumer of the database JSON API (or any similar API)

## Obtaining API URL and Auth

1. **User-provided**: If the user gives a base URL (e.g. `http://localhost:8000` or `https://api.example.com`) and optionally a key or token, use them directly.
2. **Environment variables**: `API_BASE_URL`, `API_URL`, `API_TOKEN`, `API_KEY`, `BEARER_TOKEN` (script does not load .env by default; caller can export or pass via args).
3. **Ask the user**: If the base URL is missing, ask: "Please provide the API base URL (e.g. https://api.example.com). If the API requires authentication, provide a Bearer token or API key."

**Auth options:**
- **Bearer token**: `Authorization: Bearer <token>` — use `--bearer TOKEN`.
- **API key in header**: e.g. `X-API-Key: <key>` — use `--api-key KEY` (default header `X-API-Key`) or `--api-key KEY --api-key-header "Authorization"` for custom header.

**Security**: Never hardcode credentials. Prefer env vars or pass as script arguments; avoid logging tokens.

## Prerequisites

- Python 3.8+
- `requests` — HTTP client (install with `pip install requests`)

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
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `base_url` | Yes | Base URL of the API (e.g. `http://localhost:8000`, no trailing slash) |
| `--path` | No* | Path to fetch (e.g. `/api/tables`). Can be repeated. *Required unless `--discover` |
| `--discover` | No | Try common discovery paths and list available endpoints/resources |
| `--bearer` | No | Bearer token for `Authorization: Bearer <token>` |
| `--api-key` | No | API key value (sent in header, see `--api-key-header`) |
| `--api-key-header` | No | Header name for API key (default: `X-API-Key`) |
| `--output` | No | Write result to this JSON file (default: stdout) |
| `--timeout` | No | Request timeout in seconds (default: 30) |

If both `--path` and `--discover` are used, the script runs discovery first, then fetches the given paths.

## Output Format

- **Single path**: `{"url": "...", "path": "...", "status_code": 200, "data": <response JSON>}`  
- **Multiple paths**: `{"paths": [ {"path": "...", "status_code": 200, "data": ...}, ... ]}`  
- **Discover**: `{"base_url": "...", "discovery": [ {"path": "...", "status_code": 200, "data": ... }, ... ], "tables": ["customers", ...] }` when `/api/tables` returns a list; otherwise discovery entries only.

## Script Reference

Implementation: `.cursor/skills/api-reader/scripts/api_reader.py`
