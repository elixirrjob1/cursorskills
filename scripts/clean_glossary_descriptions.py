#!/usr/bin/env python3
"""Clean glossary term descriptions in OpenMetadata by removing inline metadata."""

from __future__ import annotations

import base64
import json
import os
import re
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

API_VERSION_PREFIX = "v1"
_TOKEN_CACHE: dict[str, Any] = {"token": None}


def _api_root() -> str:
    base = os.getenv("OPENMETADATA_BASE_URL", "").strip().rstrip("/")
    if not base:
        raise RuntimeError("Missing OPENMETADATA_BASE_URL")
    return f"{base}/api" if not base.endswith("/api") else base


def _api_url(path: str) -> str:
    cleaned = path.lstrip("/")
    if not cleaned.startswith(f"{API_VERSION_PREFIX}/"):
        cleaned = f"{API_VERSION_PREFIX}/{cleaned}"
    return f"{_api_root()}/{cleaned}"


def _login() -> str:
    jwt = os.getenv("OPENMETADATA_JWT_TOKEN", "").strip()
    if jwt:
        return jwt
    cached = _TOKEN_CACHE.get("token")
    if isinstance(cached, str) and cached.strip():
        return cached.strip()
    email = os.getenv("OPENMETADATA_EMAIL", "").strip()
    password = os.getenv("OPENMETADATA_PASSWORD", "")
    encoded = base64.b64encode(password.encode()).decode("ascii")
    for payload in [{"email": email, "password": encoded}, {"email": email, "password": password}]:
        try:
            r = requests.post(
                _api_url("users/login"),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json=payload, timeout=(10, 30),
            )
            r.raise_for_status()
            data = r.json() if r.content else {}
            for key in ("accessToken", "jwtToken", "token", "id_token"):
                tok = data.get(key)
                if isinstance(tok, str) and tok.strip():
                    _TOKEN_CACHE["token"] = tok.strip()
                    return tok.strip()
        except Exception:
            continue
    raise RuntimeError("OpenMetadata login failed")


def _headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_login()}",
    }


def _get(endpoint: str, params: dict | None = None) -> Any:
    for attempt in range(2):
        try:
            r = requests.get(_api_url(endpoint), headers=_headers(), params=params, timeout=(10, 30))
            if r.status_code == 401 and attempt < 1:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"GET failed: {e}") from e
            time.sleep(1)


def _patch(endpoint: str, ops: list[dict[str, Any]]) -> Any:
    headers = dict(_headers())
    headers["Content-Type"] = "application/json-patch+json"
    for attempt in range(2):
        try:
            r = requests.patch(_api_url(endpoint), headers=headers, json=ops, timeout=(10, 30))
            if r.status_code == 401 and attempt < 1:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            return r.json() if r.content else {"success": True}
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"PATCH failed: {e}") from e
            time.sleep(1)


def clean_description(desc: str) -> str:
    if not desc:
        return desc

    # Remove '**Type:** ... | **Usage:** ...' inline metadata (retail pattern)
    cleaned = re.sub(r"\s*\*\*Type:\*\*[^|]*\|\s*\*\*Usage:\*\*[^\n]*", "", desc)

    # Split off 'Business usage:' block (lending pattern)
    cleaned = re.split(r"\n\s*Business usage:", cleaned)[0]

    # Split off 'Term type:' block (lending pattern)
    cleaned = re.split(r"\n\s*Term type:", cleaned)[0]

    # Split off 'Inferred ...' paragraphs
    cleaned = re.split(r"\n\s*\nInferred[\s;]", cleaned)[0]

    # Split off 'Review status:' paragraphs
    cleaned = re.split(r"\n\s*\nReview status:", cleaned)[0]
    cleaned = re.split(r"\nReview status:", cleaned)[0]

    return cleaned.strip()


def main() -> None:
    after = None
    all_terms: list[dict] = []

    while True:
        params: dict[str, Any] = {"limit": 100}
        if after:
            params["after"] = after
        resp = _get("glossaryTerms", params=params)
        batch = resp.get("data", [])
        all_terms.extend(batch)
        paging = resp.get("paging", {})
        after = paging.get("after")
        if not after or not batch:
            break

    print(f"Found {len(all_terms)} glossary terms total")
    print()

    updated = 0
    skipped = 0
    errors = []

    for term in all_terms:
        term_id = term.get("id", "")
        fqn = term.get("fullyQualifiedName", "")
        name = fqn.split(".")[-1] if fqn else term.get("name", "")
        original = term.get("description", "")
        cleaned = clean_description(original)

        if cleaned == original:
            skipped += 1
            continue

        try:
            _patch(f"glossaryTerms/{term_id}", [
                {"op": "replace", "path": "/description", "value": cleaned},
            ])
            print(f"  UPDATED {name}")
            print(f"    was: {original[:100]}...")
            print(f"    now: {cleaned[:100]}")
            print()
            updated += 1
        except Exception as e:
            msg = f"FAIL {name}: {e}"
            print(f"  {msg}")
            errors.append(msg)

    print("=" * 60)
    print(f"Updated: {updated}")
    print(f"Skipped (already clean): {skipped}")
    print(f"Errors:  {len(errors)}")
    if errors:
        for err in errors:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
