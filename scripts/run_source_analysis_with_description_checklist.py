#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
ANALYZER_PATH = REPO_ROOT / ".cursor" / "skills" / "source-system-analyser" / "scripts" / "source_system_analyzer.py"
CHECKLIST_PATH = SCRIPTS_DIR / "build_description_enrichment_checklist.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    spec.loader.exec_module(module)
    return module


def _resolved_output_path(output_path: str, schema_document: dict) -> Path:
    out_path = Path(output_path)
    base = out_path.stem
    ext = out_path.suffix or ".json"
    schema_name = str(schema_document.get("metadata", {}).get("schema_filter") or "public")
    dialect = str(schema_document.get("connection", {}).get("driver") or "unknown")
    return out_path.parent / f"{base}_{schema_name}_{dialect}{ext}"


def main() -> None:
    analyzer = _load_module("source_system_analyzer_wrapper", ANALYZER_PATH)
    checklist_builder = _load_module("build_description_enrichment_checklist_wrapper", CHECKLIST_PATH)

    analyzer._load_env_file()

    parser = argparse.ArgumentParser(
        description="Run source-system analysis and automatically build the description enrichment checklist against the final output JSON."
    )
    parser.add_argument(
        "database_url",
        nargs="?",
        default=None,
        help="Database URL, or keyvault://<secret-name> reference (optional when --database-url-secret is used)",
    )
    parser.add_argument("output_json_path", help="Base path for schema.json output")
    parser.add_argument("schema", nargs="?", default=None, help="Schema to analyze")
    parser.add_argument("--dialect", choices=["postgresql", "mssql", "oracle"], default=None)
    parser.add_argument("--database-url-secret", default=None)
    parser.add_argument("--keyvault-name", default=None)
    parser.add_argument(
        "--checklist-output",
        default=None,
        help="Optional explicit checklist output path",
    )
    args = parser.parse_args()

    database_url = analyzer._resolve_database_url(
        args.database_url,
        database_url_secret=args.database_url_secret,
        keyvault_name=args.keyvault_name,
    )
    schema = args.schema or os.environ.get("DATABASE_SCHEMA") or os.environ.get("SCHEMA")

    result = analyzer.analyze_source_system(
        database_url,
        args.output_json_path,
        schema=schema,
        dialect_override=args.dialect,
        generate_missing_descriptions=False,
    )
    if result.get("error"):
        print(json.dumps(result, indent=2))
        raise SystemExit(1)

    final_schema_path = _resolved_output_path(args.output_json_path, result)
    checklist = checklist_builder.build_checklist(result, str(final_schema_path))

    checklist_output = (
        Path(args.checklist_output)
        if args.checklist_output
        else final_schema_path.with_name(f"{final_schema_path.stem}_description_checklist.json")
    )
    checklist_output.write_text(json.dumps(checklist, indent=2), encoding="utf-8")

    print(json.dumps(
        {
            "schema_json": str(final_schema_path),
            "checklist_json": str(checklist_output),
            "missing_table_descriptions": checklist["summary"]["missing_table_descriptions"],
            "missing_column_descriptions": checklist["summary"]["missing_column_descriptions"],
            "next_step": (
                "If checklist items remain, Cursor must complete column descriptions first for each table, "
                "then table descriptions, then run scripts/apply_description_enrichment.py."
            ),
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
