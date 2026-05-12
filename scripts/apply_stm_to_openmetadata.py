#!/usr/bin/env python3
"""Apply STM markdown files to OpenMetadata.

Parses Sections 2, 4, 5, 6, 7 from each STM and issues JSON-patch calls that:
  - Set table description (Section 2 blockquote)
  - Assign table-level classification tags (Section 5 Scope=Table)
  - Assign column-level classification tags (Section 5 Scope=Column)
  - Assign table-level glossary terms (Section 6 Scope=Table)
  - Assign column-level glossary terms (Section 6 Scope=Column)
  - Set column descriptions from Section 7 Final mapping

Idempotent — existing tags/glossary terms are preserved; descriptions overwritten.

Usage:
  python scripts/apply_stm_to_openmetadata.py \\
      --stm-dir stm/output \\
      --service snowflake_fivetran \\
      --database DRIP_DATA_INTELLIGENCE \\
      --schema DBT_PROD_ENRICHED

  # For views in DBT_PROD (maps DimCustomer → vw_DimCustomer):
  python scripts/apply_stm_to_openmetadata.py \\
      --stm-dir stm/output \\
      --service snowflake_fivetran \\
      --database DRIP_DATA_INTELLIGENCE \\
      --schema DBT_PROD \\
      --table-prefix vw_
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

API_VERSION_PREFIX = "v1"
_TOKEN_CACHE: dict[str, Any] = {"token": None}


# ---------------------------------------------------------------------------
# OpenMetadata REST helpers
# ---------------------------------------------------------------------------

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
    if not email or not password:
        raise RuntimeError("Missing OPENMETADATA_EMAIL / OPENMETADATA_PASSWORD")
    encoded = base64.b64encode(password.encode()).decode("ascii")
    for payload in [{"email": email, "password": encoded}, {"email": email, "password": password}]:
        try:
            r = requests.post(
                _api_url("users/login"),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json=payload, timeout=(10, 30),
            )
            r.raise_for_status()
            for key in ("accessToken", "jwtToken", "token", "id_token"):
                tok = (r.json() if r.content else {}).get(key)
                if isinstance(tok, str) and tok.strip():
                    _TOKEN_CACHE["token"] = tok.strip()
                    return tok.strip()
        except Exception:
            continue
    raise RuntimeError("OpenMetadata login failed")


def _headers(ct: str = "application/json") -> dict[str, str]:
    return {"Accept": "application/json", "Content-Type": ct,
            "Authorization": f"Bearer {_login()}"}


def _get(path: str, params: dict | None = None) -> Any:
    for attempt in range(2):
        try:
            r = requests.get(_api_url(path), headers=_headers(), params=params, timeout=(10, 30))
            if r.status_code == 401 and attempt == 0:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"GET {path} failed: {e}") from e
            time.sleep(1)


def _patch(path: str, ops: list[dict]) -> Any:
    for attempt in range(2):
        try:
            r = requests.patch(
                _api_url(path), headers=_headers("application/json-patch+json"),
                json=ops, timeout=(10, 30),
            )
            if r.status_code == 401 and attempt == 0:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            return r.json() if r.content else {"success": True}
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"PATCH {path} failed: {e}") from e
            time.sleep(1)


# ---------------------------------------------------------------------------
# STM parsing
# ---------------------------------------------------------------------------

def _extract_section(text: str, section_num: int) -> str:
    """Return the raw text of a numbered section."""
    pattern = rf"##\s+{section_num}\."
    parts = re.split(r"(?m)^##\s+\d+\.", text)
    headers = re.findall(r"(?m)^##\s+\d+\.", text)
    for i, hdr in enumerate(headers):
        if re.match(pattern, hdr):
            return parts[i + 1] if i + 1 < len(parts) else ""
    return ""


def _parse_md_table(text: str) -> list[dict[str, str]]:
    """Parse a markdown table into a list of dicts."""
    rows: list[dict[str, str]] = []
    headers: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if all(re.match(r"^[-: ]+$", c) for c in cells if c):
            continue
        if not headers:
            headers = cells
        else:
            rows.append(dict(zip(headers, cells)))
    return rows


def parse_stm(path: Path) -> dict[str, Any]:
    """Return a dict with keys: table_name, description, table_tags,
    col_tags, table_glossary, col_glossary, col_descriptions."""
    text = path.read_text(encoding="utf-8")

    # Section 2 — description (first blockquote line)
    sec2 = _extract_section(text, 2)
    desc_match = re.search(r"^>\s*(.+)", sec2, re.MULTILINE)
    description = desc_match.group(1).strip() if desc_match else ""

    # Section 4 — target table name (row 0, "Table Name" column)
    sec4 = _extract_section(text, 4)
    sec4_rows = _parse_md_table(sec4)
    table_name = ""
    for row in sec4_rows:
        # header key may vary in capitalisation
        for k, v in row.items():
            if "table" in k.lower() and "name" in k.lower() and v.strip():
                table_name = v.strip()
                break
        if table_name:
            break

    # Section 5 — classification tags
    sec5 = _extract_section(text, 5)
    sec5_rows = _parse_md_table(sec5)
    table_tags: list[str] = []
    col_tags: dict[str, list[str]] = {}
    for row in sec5_rows:
        scope = row.get("Scope", "").strip()
        col = row.get("Column", "").strip()
        fqn = row.get("Tag FQN", "").strip()
        if not fqn:
            continue
        if scope.lower() == "table":
            if fqn not in table_tags:
                table_tags.append(fqn)
        elif scope.lower() == "column" and col:
            col_tags.setdefault(col, [])
            if fqn not in col_tags[col]:
                col_tags[col].append(fqn)

    # Section 6 — glossary terms
    sec6 = _extract_section(text, 6)
    sec6_rows = _parse_md_table(sec6)
    table_glossary: list[str] = []
    col_glossary: dict[str, list[str]] = {}
    for row in sec6_rows:
        scope = row.get("Scope", "").strip()
        col = row.get("Column", "").strip()
        fqn = row.get("Term FQN", "").strip()
        if not fqn:
            continue
        if scope.lower() == "table":
            if fqn not in table_glossary:
                table_glossary.append(fqn)
        elif scope.lower() == "column" and col:
            col_glossary.setdefault(col, [])
            if fqn not in col_glossary[col]:
                col_glossary[col].append(fqn)

    # Section 7 — Final column descriptions
    sec7 = _extract_section(text, 7)
    col_descriptions: dict[str, str] = {}
    # Prefer the explicit "### Final" sub-section; fall back to the flat mapping
    # table directly under ## 7 for STMs that don't have sub-headings.
    final_match = re.search(r"###\s+Final\s*\n(.*?)(?=###|\Z)", sec7, re.DOTALL)
    mapping_text = final_match.group(1) if final_match else sec7
    for row in _parse_md_table(mapping_text):
        # Column name key varies: "Target Column", "Column"
        col_key = next((k for k in row if "column" in k.lower() and "target" in k.lower()), None)
        if col_key is None:
            col_key = next((k for k in row if "column" in k.lower()), None)
        desc_key = next((k for k in row if "description" in k.lower()), None)
        if not col_key or not desc_key:
            continue
        col_raw = row[col_key].strip()
        # Strip schema prefix like "GOLD.DimCustomer" to get just the column
        if "." in col_raw:
            col_raw = col_raw.split(".")[-1].strip()
        desc_val = row[desc_key].strip()
        if col_raw and desc_val:
            col_descriptions[col_raw] = desc_val

    return {
        "table_name": table_name,
        "description": description,
        "table_tags": table_tags,
        "col_tags": col_tags,
        "table_glossary": table_glossary,
        "col_glossary": col_glossary,
        "col_descriptions": col_descriptions,
    }


# ---------------------------------------------------------------------------
# Tag / glossary label builders
# ---------------------------------------------------------------------------

def _tag_label(fqn: str, source: str = "Classification") -> dict:
    return {"tagFQN": fqn, "source": source, "labelType": "Manual", "state": "Confirmed"}


def _merge_tags(existing: list[dict] | None, new_fqns: list[str],
                source: str = "Classification") -> list[dict]:
    merged = list(existing or [])
    seen = {(t.get("tagFQN", ""), t.get("source", "")) for t in merged}
    for fqn in new_fqns:
        key = (fqn, source)
        if key not in seen:
            merged.append(_tag_label(fqn, source))
            seen.add(key)
    return merged


# ---------------------------------------------------------------------------
# Normalised column name lookup
# ---------------------------------------------------------------------------

def _build_col_index(om_columns: list[dict]) -> dict[str, int]:
    """Upper-case key → column list index."""
    return {c["name"].upper(): i for i, c in enumerate(om_columns)}


def _resolve_col(name: str, index: dict[str, int]) -> int | None:
    return index.get(name.upper())


# ---------------------------------------------------------------------------
# Apply one STM to one OM table
# ---------------------------------------------------------------------------

def apply_stm(stm: dict[str, Any], om_table: dict) -> dict[str, Any]:
    """Issue JSON-patch calls for description, tags, glossary, col descs.
    Returns a stats dict."""
    table_id = om_table["id"]
    om_cols: list[dict] = om_table.get("columns", [])
    col_idx = _build_col_index(om_cols)
    stats = {"desc": False, "table_tags": 0, "col_tags": 0,
             "table_gloss": 0, "col_gloss": 0, "col_descs": 0,
             "missing_cols": [], "errors": []}

    # 1. Table description (use "add" — works whether field is null or set)
    if stm["description"]:
        try:
            _patch(f"tables/{table_id}", [
                {"op": "add", "path": "/description", "value": stm["description"]}
            ])
            stats["desc"] = True
        except Exception as e:
            stats["errors"].append(f"desc: {e}")

    # 2. Table-level classification tags
    if stm["table_tags"]:
        try:
            merged = _merge_tags(om_table.get("tags"), stm["table_tags"], "Classification")
            _patch(f"tables/{table_id}", [{"op": "replace", "path": "/tags", "value": merged}])
            stats["table_tags"] = len(stm["table_tags"])
        except Exception as e:
            stats["errors"].append(f"table_tags: {e}")

    # 3. Table-level glossary terms
    if stm["table_glossary"]:
        try:
            merged = _merge_tags(om_table.get("tags"), stm["table_glossary"], "Glossary")
            # glossary terms live in the same /tags array with source=Glossary
            # fetch fresh tags first to avoid clobbering step 2
            fresh = _get(f"tables/{table_id}", {"fields": "tags"})
            merged = _merge_tags(fresh.get("tags"), stm["table_glossary"], "Glossary")
            _patch(f"tables/{table_id}", [{"op": "replace", "path": "/tags", "value": merged}])
            stats["table_gloss"] = len(stm["table_glossary"])
        except Exception as e:
            stats["errors"].append(f"table_glossary: {e}")

    # 4. Per-column tags, glossary terms, and descriptions
    for col_name, col_tag_fqns in stm["col_tags"].items():
        idx = _resolve_col(col_name, col_idx)
        if idx is None:
            if col_name not in stats["missing_cols"]:
                stats["missing_cols"].append(col_name)
            continue
        try:
            merged = _merge_tags(om_cols[idx].get("tags"), col_tag_fqns, "Classification")
            _patch(f"tables/{table_id}",
                   [{"op": "replace", "path": f"/columns/{idx}/tags", "value": merged}])
            stats["col_tags"] += 1
        except Exception as e:
            stats["errors"].append(f"col_tags {col_name}: {e}")

    for col_name, col_gloss_fqns in stm["col_glossary"].items():
        idx = _resolve_col(col_name, col_idx)
        if idx is None:
            if col_name not in stats["missing_cols"]:
                stats["missing_cols"].append(col_name)
            continue
        try:
            fresh_col = _get(f"tables/{table_id}", {"fields": "columns"})
            fresh_cols = fresh_col.get("columns", [])
            fresh_idx = _build_col_index(fresh_cols)
            fi = fresh_idx.get(col_name.upper(), idx)
            existing = fresh_cols[fi].get("tags") if fi < len(fresh_cols) else []
            merged = _merge_tags(existing, col_gloss_fqns, "Glossary")
            _patch(f"tables/{table_id}",
                   [{"op": "replace", "path": f"/columns/{fi}/tags", "value": merged}])
            stats["col_gloss"] += 1
        except Exception as e:
            stats["errors"].append(f"col_glossary {col_name}: {e}")

    for col_name, col_desc in stm["col_descriptions"].items():
        if not col_desc:
            continue
        idx = _resolve_col(col_name, col_idx)
        if idx is None:
            if col_name not in stats["missing_cols"]:
                stats["missing_cols"].append(col_name)
            continue
        try:
            _patch(f"tables/{table_id}",
                   [{"op": "add", "path": f"/columns/{idx}/description", "value": col_desc}])
            stats["col_descs"] += 1
        except Exception as e:
            stats["errors"].append(f"col_desc {col_name}: {e}")

    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Apply STM files to OpenMetadata.")
    ap.add_argument("--stm-dir", default="stm/output", help="Folder with STM .md files")
    ap.add_argument("--stm", default=None, help="Single STM file (overrides --stm-dir)")
    ap.add_argument("--service", default="snowflake_fivetran")
    ap.add_argument("--database", default="DRIP_DATA_INTELLIGENCE")
    ap.add_argument("--schema", required=True, help="Target schema name in OpenMetadata")
    ap.add_argument("--table-fqn", default=None,
                    help="Explicit table FQN (single-STM mode only)")
    ap.add_argument("--table-prefix", default="",
                    help="Prefix to prepend to STM table name, e.g. 'vw_'")
    args = ap.parse_args()

    stm_paths: list[Path] = []
    if args.stm:
        stm_paths = [Path(args.stm)]
    else:
        stm_paths = sorted(
            p for p in Path(args.stm_dir).glob("*.md")
            if p.name.lower() != "readme.md"
        )

    if not stm_paths:
        print(f"No STM files found in {args.stm_dir}")
        sys.exit(1)

    schema_fqn = f"{args.service}.{args.database}.{args.schema}"
    print(f"Loading OpenMetadata tables from {schema_fqn} …")
    om_resp = _get("tables", {"databaseSchema": schema_fqn, "fields": "columns,tags", "limit": 500})
    om_tables_list = om_resp.get("data", [])
    om_lookup: dict[str, dict] = {t["name"].upper(): t for t in om_tables_list}
    print(f"  Found {len(om_lookup)} tables/views in schema\n")

    total = {"desc": 0, "table_tags": 0, "col_tags": 0,
             "table_gloss": 0, "col_gloss": 0, "col_descs": 0,
             "missing_cols": 0, "errors": 0, "skipped": 0}

    for stm_path in stm_paths:
        stm = parse_stm(stm_path)
        raw_name = stm["table_name"]
        if not raw_name:
            print(f"  SKIP {stm_path.name} — could not parse table name from Section 4")
            total["skipped"] += 1
            continue

        if args.table_fqn:
            # single-table override: derive name from last FQN segment
            lookup_name = args.table_fqn.split(".")[-1].upper()
        else:
            lookup_name = (args.table_prefix + raw_name).upper()

        if lookup_name not in om_lookup:
            print(f"  SKIP {raw_name} → {lookup_name} not in OpenMetadata ({schema_fqn})")
            total["skipped"] += 1
            continue

        om_table = om_lookup[lookup_name]
        display = f"{args.table_prefix}{raw_name}"
        print(f"  Applying {stm_path.name} → {display} …", end=" ", flush=True)
        try:
            s = apply_stm(stm, om_table)
        except Exception as e:
            print(f"[FAIL] {e}")
            total["errors"] += 1
            continue

        parts = []
        if s["desc"]:
            parts.append("desc")
        if s["table_tags"]:
            parts.append(f"{s['table_tags']} tbl-tags")
        if s["col_tags"]:
            parts.append(f"{s['col_tags']} col-tags")
        if s["table_gloss"]:
            parts.append(f"{s['table_gloss']} tbl-gloss")
        if s["col_gloss"]:
            parts.append(f"{s['col_gloss']} col-gloss")
        if s["col_descs"]:
            parts.append(f"{s['col_descs']} col-descs")
        status = "[OK] " + ", ".join(parts) if parts else "[OK] (nothing to apply)"
        if s["missing_cols"]:
            status += f" | MISSING cols: {s['missing_cols']}"
        if s["errors"]:
            status += f" | ERRORS: {s['errors']}"
        print(status)

        total["desc"] += int(s["desc"])
        total["table_tags"] += s["table_tags"]
        total["col_tags"] += s["col_tags"]
        total["table_gloss"] += s["table_gloss"]
        total["col_gloss"] += s["col_gloss"]
        total["col_descs"] += s["col_descs"]
        total["missing_cols"] += len(s["missing_cols"])
        total["errors"] += len(s["errors"])

    print(f"\n{'=' * 60}")
    print(f"STMs processed : {len(stm_paths) - total['skipped']}")
    print(f"Skipped        : {total['skipped']}")
    print(f"Table descs    : {total['desc']}")
    print(f"Table tags     : {total['table_tags']}")
    print(f"Table glossary : {total['table_gloss']}")
    print(f"Column tags    : {total['col_tags']}")
    print(f"Column glossary: {total['col_gloss']}")
    print(f"Column descs   : {total['col_descs']}")
    print(f"Missing cols   : {total['missing_cols']}")
    print(f"Errors         : {total['errors']}")
    if total["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
