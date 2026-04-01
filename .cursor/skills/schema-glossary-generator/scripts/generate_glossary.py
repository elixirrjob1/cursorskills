#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill


ROOT_DIR = Path(__file__).resolve().parents[4]
LATEST_SCHEMA_DIR = ROOT_DIR / "LATEST_SCHEMA"

HEADER_FILL = PatternFill(fill_type="solid", start_color="1F4E78", end_color="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)


def _slug_to_label(value):
    text = str(value or "").strip().replace(".", " ").replace("_", " ")
    words = [word for word in text.split() if word]
    if not words:
        return ""
    if len(words) == 1:
        return words[0].title()
    return " ".join(word.capitalize() if word.lower() not in {"and", "or", "of"} else word.lower() for word in words)


def _plural_to_singular(label):
    text = (label or "").strip()
    if not text:
        return text
    lower = text.lower()
    if lower.endswith("ies") and len(text) > 3:
        return text[:-3] + "y"
    if lower.endswith("ses") and len(text) > 3:
        return text[:-2]
    if lower.endswith("s") and not lower.endswith("ss") and len(text) > 1:
        return text[:-1]
    return text


def _canonical_term(value):
    return " ".join(str(value or "").strip().lower().replace("_", " ").replace(".", " ").split())


def _term_type_from_concept(concept_id):
    concept_id = str(concept_id or "").lower()
    if concept_id.startswith("finance."):
        return "business_measure"
    if concept_id.startswith("identifier."):
        return "identifier"
    if concept_id.startswith("entity.status"):
        return "status"
    if concept_id.startswith("contact.") or concept_id.startswith("temporal.") or concept_id.startswith("entity."):
        return "business_attribute"
    return "inferred_concept"


def _confidence_tier(score):
    if score >= 0.85:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def _table_term_type(table_name):
    name = str(table_name or "").lower()
    if name.endswith("_orders") or name.endswith("_order_items") or name == "inventory":
        return "business_process"
    return "business_entity"


def _column_term_type(column):
    concept_id = column.get("concept_id")
    if concept_id:
        return _term_type_from_concept(concept_id)

    semantic_class = str(column.get("semantic_class") or "").lower()
    name = str(column.get("name") or "").lower()
    if "amount" in semantic_class or "price" in name or "amount" in name or "cost" in name:
        return "business_measure"
    if "status" in name:
        return "status"
    if name.endswith("_id") or semantic_class.endswith("_identifier"):
        return "identifier"
    return "business_attribute"


def _term_from_table(table_name):
    return _plural_to_singular(_slug_to_label(table_name))


def _term_from_column(column_name):
    return _slug_to_label(column_name)


def _term_from_concept(concept):
    concept_id = str(concept.get("concept_id") or "")
    alias_groups = [str(item).strip() for item in concept.get("alias_groups") or [] if str(item).strip()]
    if concept_id:
        last_token = concept_id.split(".")[-1]
        if last_token == "person_name":
            return "Person Name"
        if last_token == "currency_amount":
            return "Currency Amount"
        if last_token == "foreign_key":
            return "Foreign Key Identifier"
        return _slug_to_label(last_token)
    if alias_groups:
        return _slug_to_label(alias_groups[0])
    return ""


def _status_from_confidence(score):
    return "confirmed_from_schema" if score >= 0.85 else "inferred_from_schema"


def _append_unique(target_list, values):
    seen = set(target_list)
    for value in values:
        if not value:
            continue
        if value not in seen:
            target_list.append(value)
            seen.add(value)


def _infer_table_usage(table):
    table_name = str(table.get("table") or "")
    description = str(table.get("table_description") or "").strip()
    foreign_keys = table.get("foreign_keys") or []
    columns = table.get("columns") or []
    related_tables = []
    for fk in foreign_keys:
        ref = str(fk.get("references") or "")
        if "." in ref:
            related_tables.append(ref.split(".", 1)[0])

    if table_name == "sales_orders":
        return "Used to track customer sales transactions, order ownership, order timing, and order totals."
    if table_name == "purchase_orders":
        return "Used in procurement workflow to track orders placed with suppliers, expected delivery dates, and approval relationships."
    if table_name == "inventory":
        return "Used to monitor stock on hand, reorder thresholds, and product availability by store."
    if table_name.endswith("_order_items"):
        return "Used to store line-level details for transaction items, including product, quantity, and pricing."
    if related_tables:
        labels = ", ".join(_slug_to_label(item) for item in sorted(set(related_tables)))
        return f"Used with related records such as {labels} to support operational reporting and joins."
    if description:
        return description
    if columns:
        sample_columns = ", ".join(_slug_to_label(col.get("name")) for col in columns[:3])
        return f"Used to manage { _slug_to_label(table_name).lower() } records and fields such as {sample_columns.lower()}."
    return f"Used to manage { _slug_to_label(table_name).lower() } records."


def _infer_column_usage(table, column):
    table_label = _slug_to_label(table.get("table"))
    column_name = str(column.get("name") or "")
    name = column_name.lower()
    if name == "status":
        return f"Used to indicate the current lifecycle state of the {table_label.lower()} record."
    if name in {"created_at", "updated_at", "order_date", "expected_date", "hire_date", "last_restocked_at"}:
        return f"Used in reporting and operational tracking for {table_label.lower()} timing."
    if "amount" in name or "price" in name or "cost" in name:
        return f"Used in financial reporting and valuation for {table_label.lower()} activity."
    if name.endswith("_id"):
        return f"Used to identify or join the related {table_label.lower()} record."
    return f"Used as a business attribute on {table_label.lower()} records."


def _compact_list(values):
    return [value for value in values if value]


def _build_entry(term, term_type, definition, business_usage, confidence, inference_basis, status, source_table=None, source_column=None, synonyms=None, notes=None):
    entry = {
        "term": term,
        "term_type": term_type,
        "definition": definition.strip(),
        "business_usage": business_usage.strip(),
        "synonyms": _compact_list(synonyms or []),
        "source_tables": _compact_list([source_table] if source_table else []),
        "source_columns": _compact_list([source_column] if source_column else []),
        "confidence": round(float(confidence), 2),
        "confidence_tier": _confidence_tier(float(confidence)),
        "inference_basis": inference_basis.strip(),
        "source_refs": _compact_list([f"{source_table}.{source_column}" if source_table and source_column else source_table]),
        "notes": notes or "",
        "status": status,
    }
    return entry


def _register_entry(bucket, entry):
    key = _canonical_term(entry["term"])
    if not key:
        return
    if key not in bucket:
        bucket[key] = deepcopy(entry)
        return

    existing = bucket[key]
    if entry["confidence"] > existing["confidence"]:
        preferred = deepcopy(entry)
        preferred["source_tables"] = existing["source_tables"]
        preferred["source_columns"] = existing["source_columns"]
        preferred["source_refs"] = existing["source_refs"]
        preferred["synonyms"] = existing["synonyms"]
        bucket[key] = preferred
        existing = bucket[key]

    _append_unique(existing["source_tables"], entry["source_tables"])
    _append_unique(existing["source_columns"], entry["source_columns"])
    _append_unique(existing["source_refs"], entry["source_refs"])
    _append_unique(existing["synonyms"], entry["synonyms"])

    if entry["confidence"] > existing["confidence"]:
        existing["confidence"] = entry["confidence"]
    existing["confidence_tier"] = _confidence_tier(existing["confidence"])
    if existing["status"] != "confirmed_from_schema":
        existing["status"] = entry["status"]
    if not existing["notes"] and entry["notes"]:
        existing["notes"] = entry["notes"]


def _concept_entries(payload):
    entries = []
    registry = payload.get("concept_registry") or {}
    for concept in registry.get("concepts") or []:
        term = _term_from_concept(concept)
        if not term:
            continue
        sample_columns = [str(item) for item in concept.get("sample_columns") or [] if item]
        sample_tables = sorted({item.split(".", 1)[0] for item in sample_columns if "." in item})
        definition = (
            f"Schema-derived concept representing {term.lower()} fields that appear consistently across related tables."
        )
        usage = "Used to standardize comparable fields across the source model for joins, search, and reporting."
        alias_groups = [_slug_to_label(item) for item in concept.get("alias_groups") or []]
        entries.append(
            _build_entry(
                term=term,
                term_type=_term_type_from_concept(concept.get("concept_id")),
                definition=definition,
                business_usage=usage,
                confidence=concept.get("avg_confidence") or 0.5,
                inference_basis=f"Derived from concept_registry concept_id={concept.get('concept_id')} across {concept.get('table_count', 0)} tables.",
                status=_status_from_confidence(concept.get("avg_confidence") or 0.5),
                synonyms=alias_groups,
                notes=f"Signals: {', '.join(str(item) for item in concept.get('signals') or [])}",
            )
        )
        entries[-1]["source_tables"] = sample_tables
        entries[-1]["source_columns"] = sample_columns
        entries[-1]["source_refs"] = sample_columns
    return entries


def _table_entries(payload):
    entries = []
    for table in payload.get("tables") or []:
        table_name = str(table.get("table") or "")
        if not table_name:
            continue
        description = str(table.get("table_description") or "").strip()
        term = _term_from_table(table_name)
        confidence = 0.88 if description else 0.72
        definition = description or f"Business records for {term.lower()}."
        entries.append(
            _build_entry(
                term=term,
                term_type=_table_term_type(table_name),
                definition=definition,
                business_usage=_infer_table_usage(table),
                confidence=confidence,
                inference_basis=f"Derived from table={table_name}, table_description, foreign keys, and column set.",
                status=_status_from_confidence(confidence),
                source_table=table_name,
                synonyms=[_slug_to_label(table_name)],
            )
        )
    return entries


def _should_emit_column_entry(column):
    name = str(column.get("name") or "").lower()
    concept_id = str(column.get("concept_id") or "").lower()
    semantic_class = str(column.get("semantic_class") or "").lower()
    if name in {"created_at", "updated_at"}:
        return True
    if name == "status" or name.endswith("_date"):
        return True
    if name.endswith("_id"):
        return True
    if "amount" in name or "price" in name or "cost" in name or "quantity" in name:
        return True
    if concept_id or semantic_class in {"contact", "timestamp", "currency_amount"}:
        return True
    return False


def _column_entries(payload):
    entries = []
    for table in payload.get("tables") or []:
        table_name = str(table.get("table") or "")
        for column in table.get("columns") or []:
            if not _should_emit_column_entry(column):
                continue
            column_name = str(column.get("name") or "")
            term = _term_from_column(column_name)
            definition = str(column.get("column_description") or "").strip()
            if not definition:
                definition = f"Business field representing {term.lower()} on {_slug_to_label(table_name).lower()} records."
            confidence = float(column.get("concept_confidence") or 0.68)
            entries.append(
                _build_entry(
                    term=term,
                    term_type=_column_term_type(column),
                    definition=definition,
                    business_usage=_infer_column_usage(table, column),
                    confidence=confidence,
                    inference_basis=f"Derived from {table_name}.{column_name} using column_description, concept metadata, and field naming.",
                    status=_status_from_confidence(confidence),
                    source_table=table_name,
                    source_column=column_name,
                    synonyms=[_slug_to_label(column.get("concept_alias_group") or ""), _slug_to_label(column_name)],
                    notes=f"semantic_class={column.get('semantic_class') or ''}; concept_id={column.get('concept_id') or ''}",
                )
            )
    return entries


def build_glossary(payload, source_path):
    bucket = {}
    for entry in _concept_entries(payload):
        _register_entry(bucket, entry)
    for entry in _table_entries(payload):
        _register_entry(bucket, entry)
    for entry in _column_entries(payload):
        _register_entry(bucket, entry)

    entries = sorted(bucket.values(), key=lambda item: (item["term"].lower(), item["term_type"]))
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_schema_json": str(Path(source_path)),
        "glossary_entry_count": len(entries),
        "generation_mode": "schema_only",
        "inference_mode": "aggressive",
    }
    return {"metadata": metadata, "entries": entries}


def _autofit(ws):
    for column_cells in ws.columns:
        max_length = 0
        column_letter = column_cells[0].column_letter
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))
        ws.column_dimensions[column_letter].width = min(max(max_length + 2, 12), 60)


def write_glossary_excel(glossary, output_path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Glossary"

    rows = glossary.get("entries") or []
    headers = [
        "term",
        "term_type",
        "definition",
        "business_usage",
        "synonyms",
        "source_tables",
        "source_columns",
        "confidence",
        "confidence_tier",
        "inference_basis",
        "source_refs",
        "notes",
        "status",
    ]

    ws.append(headers)
    for row in rows:
        ws.append(
            [
                row.get("term", ""),
                row.get("term_type", ""),
                row.get("definition", ""),
                row.get("business_usage", ""),
                ", ".join(row.get("synonyms") or []),
                ", ".join(row.get("source_tables") or []),
                ", ".join(row.get("source_columns") or []),
                row.get("confidence", ""),
                row.get("confidence_tier", ""),
                row.get("inference_basis", ""),
                ", ".join(row.get("source_refs") or []),
                row.get("notes", ""),
                row.get("status", ""),
            ]
        )

    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(vertical="top", wrap_text=True)

    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    _autofit(ws)

    meta_ws = wb.create_sheet("RunMetadata")
    meta_ws.append(["Field", "Value"])
    for cell in meta_ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
    for key, value in glossary.get("metadata", {}).items():
        meta_ws.append([key, value])
    meta_ws.freeze_panes = "A2"
    meta_ws.auto_filter.ref = meta_ws.dimensions
    _autofit(meta_ws)

    wb.save(output_path)


def default_input_path():
    candidates = sorted(LATEST_SCHEMA_DIR.glob("schema*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No schema JSON files found in {LATEST_SCHEMA_DIR}")
    return candidates[0]


def output_paths(input_path):
    input_path = Path(input_path)
    stem = input_path.stem
    return (
        input_path.with_name(f"{stem}_glossary.json"),
        input_path.with_name(f"{stem}_glossary.xlsx"),
    )


def run(input_path):
    source_path = Path(input_path) if input_path else default_input_path()
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    glossary = build_glossary(payload, source_path)
    json_output, excel_output = output_paths(source_path)
    json_output.write_text(json.dumps(glossary, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    write_glossary_excel(glossary, excel_output)
    return json_output, excel_output, glossary


def main():
    parser = argparse.ArgumentParser(description="Generate glossary JSON and Excel from analyzer schema JSON.")
    parser.add_argument("input_json", nargs="?", help="Path to analyzer schema JSON. Defaults to newest schema*.json in LATEST_SCHEMA.")
    args = parser.parse_args()
    json_output, excel_output, glossary = run(args.input_json)
    print(
        json.dumps(
            {
                "json_output": str(json_output),
                "excel_output": str(excel_output),
                "glossary_entry_count": glossary["metadata"]["glossary_entry_count"],
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
