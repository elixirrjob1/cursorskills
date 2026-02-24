#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from copy import deepcopy

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill


def _cell_value(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, dict):
        pairs = [f"{k}={_cell_value(v)}" for k, v in value.items()]
        return "; ".join(pairs)
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(_cell_value(v)) for v in value)
    return value


def _is_nested(value):
    return isinstance(value, (dict, list))


def _flatten_dict(value, prefix=""):
    out = {}
    if not isinstance(value, dict):
        return out
    for key, item in value.items():
        col = f"{prefix}{key}" if not prefix else f"{prefix}_{key}"
        if isinstance(item, dict):
            out.update(_flatten_dict(item, col))
        elif isinstance(item, list):
            out[col] = ", ".join(str(_cell_value(v)) for v in item)
        else:
            out[col] = _cell_value(item)
    return out


def _split_nested_sections(sheet_name, rows):
    if not rows:
        return [("Data", rows)]

    nested_fields = []
    for key in rows[0].keys():
        if any(_is_nested(row.get(key)) for row in rows):
            nested_fields.append(key)

    if not nested_fields:
        return [("Data", rows)]

    main_rows = []
    for row in rows:
        new_row = deepcopy(row)
        for field in nested_fields:
            nested_value = row.get(field)
            if isinstance(nested_value, list):
                new_row[f"{field}_count"] = len(nested_value)
            elif isinstance(nested_value, dict):
                new_row[f"{field}_count"] = len(nested_value.keys())
            else:
                new_row[f"{field}_count"] = 0
            new_row.pop(field, None)
        main_rows.append(new_row)

    scalar_fields = [k for k in rows[0].keys() if k not in nested_fields]
    child_sections = []

    for field in nested_fields:
        child_rows = []
        for row_idx, row in enumerate(rows, start=1):
            nested_value = row.get(field)
            if nested_value is None or nested_value == {} or nested_value == []:
                continue

            base = {"parent_row": row_idx}
            for key in scalar_fields:
                value = row.get(key)
                if not _is_nested(value):
                    base[key] = _cell_value(value)

            if isinstance(nested_value, dict):
                child = dict(base)
                child.update(_flatten_dict(nested_value))
                child_rows.append(child)
            elif isinstance(nested_value, list):
                for item_idx, item in enumerate(nested_value, start=1):
                    child = dict(base)
                    child["item_index"] = item_idx
                    if isinstance(item, dict):
                        child.update(_flatten_dict(item))
                    elif isinstance(item, list):
                        child["value"] = ", ".join(str(_cell_value(v)) for v in item)
                    else:
                        child["value"] = _cell_value(item)
                    child_rows.append(child)
            else:
                child = dict(base)
                child["value"] = _cell_value(nested_value)
                child_rows.append(child)

        if child_rows:
            child_sections.append((f"{sheet_name}.{field}", child_rows))

    return [("Data", main_rows)] + child_sections


def _join_list(values):
    if not values:
        return ""
    return ", ".join(str(v) for v in values)


def _flatten_sensitive_fields(values):
    if not isinstance(values, dict) or not values:
        return ""
    items = [f"{k}:{v}" for k, v in values.items()]
    return "; ".join(items)


def _unit_fields(column):
    unit_context = column.get("unit_context") or {}
    conversion = unit_context.get("conversion") or {}

    detected_unit = column.get("unit") or unit_context.get("detected_unit", "")
    unit_source = column.get("unit_source") or unit_context.get("detection_source", "")

    return {
        "unit": detected_unit or "",
        "unit_source": unit_source or "",
        "canonical_unit": unit_context.get("canonical_unit", ""),
        "unit_system": unit_context.get("unit_system", ""),
        "unit_confidence": unit_context.get("detection_confidence", ""),
        "unit_notes": unit_context.get("notes", ""),
        "factor_to_canonical": conversion.get("factor_to_canonical", ""),
        "offset_to_canonical": conversion.get("offset_to_canonical", ""),
        "conversion_formula": conversion.get("formula", ""),
    }


def _derive_database(connection, metadata):
    db = connection.get("database")
    if db:
        return db
    db_url = metadata.get("database_url") or ""
    if not db_url:
        return ""
    parsed = urlparse(db_url if "://" in db_url else f"dummy://{db_url}")
    if parsed.path and parsed.path != "/":
        return parsed.path.lstrip("/")
    q = parse_qs(parsed.query or "")
    service = q.get("service_name", [""])[0]
    if service:
        return service
    return ""


def _collect_sheets(payload):
    tables = payload.get("tables", []) or []
    connection = payload.get("connection", {}) or {}
    metadata = payload.get("metadata", {}) or {}
    data_quality_summary = payload.get("data_quality_summary", {}) or {}
    first_table_schema = tables[0].get("schema", "") if tables else ""

    source_name = (
        connection.get("source_name")
        or connection.get("host")
        or metadata.get("database_url")
        or ""
    )
    source_type = (
        connection.get("source_type")
        or connection.get("driver")
        or ""
    )
    schema_name = (
        connection.get("schema")
        or metadata.get("schema_filter")
        or first_table_schema
        or ""
    )

    summary = [
        {"metric": "source_name", "value": source_name},
        {"metric": "source_type", "value": source_type},
        {"metric": "database", "value": _derive_database(connection, metadata)},
        {"metric": "schema", "value": schema_name},
        {"metric": "tables_count", "value": len(tables)},
    ]
    dq_by_check_rows = []
    dq_constraints_rows = []
    dq_other_rows = []

    for key, value in data_quality_summary.items():
        if isinstance(value, dict):
            if key == "by_check":
                for check_name, check_value in value.items():
                    dq_by_check_rows.append({"check": check_name, "count": _cell_value(check_value)})
            elif key == "constraints_found":
                for constraint_name, constraint_value in value.items():
                    dq_constraints_rows.append(
                        {"constraint": constraint_name, "count": _cell_value(constraint_value)}
                    )
            else:
                for item_name, item_value in value.items():
                    dq_other_rows.append(
                        {
                            "metric_group": f"data_quality_{key}",
                            "item": item_name,
                            "value": _cell_value(item_value),
                        }
                    )
        else:
            summary.append({"metric": f"data_quality_{key}", "value": _cell_value(value)})

    table_rows = []
    column_rows = []
    join_rows = []
    fk_rows = []
    sample_rows = []
    unit_rows = []

    for table in tables:
        schema_name = table.get("schema", "")
        table_name = table.get("table", "")
        foreign_keys = table.get("foreign_keys", []) or []
        sensitive_fields = table.get("sensitive_fields", {}) or {}

        table_rows.append(
            {
                "schema": schema_name,
                "table_name": table_name,
                "row_count": table.get("row_count", ""),
                "has_primary_key": table.get("has_primary_key", ""),
                "primary_keys": _join_list(table.get("primary_keys", [])),
                "has_foreign_keys": table.get("has_foreign_keys", ""),
                "foreign_key_count": len(foreign_keys),
                "incremental_columns": _join_list(table.get("incremental_columns", [])),
                "partition_columns": _join_list(table.get("partition_columns", [])),
                "sensitive_fields": _flatten_sensitive_fields(sensitive_fields),
                "table_description": table.get("table_description", ""),
                "cdc_enabled": table.get("cdc_enabled", ""),
            }
        )

        for fk in foreign_keys:
            fk_rows.append(
                {
                    "schema": schema_name,
                    "table_name": table_name,
                    "column_name": fk.get("column", ""),
                    "references": fk.get("references", ""),
                }
            )

        for column in table.get("columns", []) or []:
            unit_data = _unit_fields(column)
            column_rows.append(
                {
                    "schema": schema_name,
                    "table_name": table_name,
                    "column_name": column.get("name", ""),
                    "data_type": column.get("type", ""),
                    "nullable": column.get("nullable", ""),
                    "is_incremental": column.get("is_incremental", ""),
                    "cardinality": column.get("cardinality", ""),
                    "null_count": column.get("null_count", ""),
                    "data_category": column.get("data_category", ""),
                    "semantic_class": column.get("semantic_class", ""),
                    "description": column.get("description", ""),
                    "unit": unit_data["unit"],
                    "unit_source": unit_data["unit_source"],
                    "canonical_unit": unit_data["canonical_unit"],
                    "unit_system": unit_data["unit_system"],
                    "unit_confidence": unit_data["unit_confidence"],
                    "unit_notes": unit_data["unit_notes"],
                    "factor_to_canonical": unit_data["factor_to_canonical"],
                    "offset_to_canonical": unit_data["offset_to_canonical"],
                    "conversion_formula": unit_data["conversion_formula"],
                    "range_min": (column.get("data_range") or {}).get("min", ""),
                    "range_max": (column.get("data_range") or {}).get("max", ""),
                }
            )

            if unit_data["unit"] or unit_data["canonical_unit"] or unit_data["unit_source"]:
                unit_rows.append(
                    {
                        "schema": schema_name,
                        "table_name": table_name,
                        "column_name": column.get("name", ""),
                        "unit": unit_data["unit"],
                        "unit_source": unit_data["unit_source"],
                        "canonical_unit": unit_data["canonical_unit"],
                        "unit_system": unit_data["unit_system"],
                        "unit_confidence": unit_data["unit_confidence"],
                        "unit_notes": unit_data["unit_notes"],
                        "factor_to_canonical": unit_data["factor_to_canonical"],
                        "offset_to_canonical": unit_data["offset_to_canonical"],
                        "conversion_formula": unit_data["conversion_formula"],
                    }
                )

        for candidate in table.get("join_candidates", []) or []:
            if isinstance(candidate, dict):
                join_rows.append(
                    {
                        "schema": schema_name,
                        "table_name": table_name,
                        "column_name": candidate.get("column", ""),
                        "target_table": candidate.get("target_table", ""),
                        "target_column": candidate.get("target_column", ""),
                        "confidence": candidate.get("confidence", ""),
                    }
                )
            else:
                join_rows.append(
                    {
                        "schema": schema_name,
                        "table_name": table_name,
                        "column_name": "",
                        "target_table": _cell_value(candidate),
                        "target_column": "",
                        "confidence": "",
                    }
                )

        sample_data = table.get("sample_data", {}) or {}
        for sample_col, values in sample_data.items():
            if not isinstance(values, list):
                values = [values]
            for idx, value in enumerate(values):
                sample_rows.append(
                    {
                        "schema": schema_name,
                        "table_name": table_name,
                        "sample_column": sample_col,
                        "sample_index": idx + 1,
                        "sample_value": _cell_value(value),
                    }
                )

    summary_sections = [("Overview", summary)]
    if dq_by_check_rows:
        summary_sections.append(("DataQualityByCheck", dq_by_check_rows))
    if dq_constraints_rows:
        summary_sections.append(("DataQualityConstraints", dq_constraints_rows))
    if dq_other_rows:
        summary_sections.append(("DataQualityDetails", dq_other_rows))

    sheets = {
        "Summary": {"sections": summary_sections},
        "Tables": table_rows,
        "Columns": column_rows,
        "JoinCandidates": join_rows,
        "ForeignKeys": fk_rows,
        "SampleData": sample_rows,
        "Units": unit_rows,
    }
    return sheets


def _style_sheet(ws):
    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="left", vertical="center")

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    for col in ws.columns:
        col_letter = col[0].column_letter
        max_len = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
        ws.column_dimensions[col_letter].width = min(max(12, max_len + 2), 80)


def _write_sheet(ws, rows):
    headers = list(rows[0].keys()) if rows else ["note"]
    ws.append(headers)
    if rows:
        for row in rows:
            ws.append([_cell_value(row.get(h, "")) for h in headers])
    else:
        ws.append(["No rows"])
    _style_sheet(ws)


def _write_multi_section_sheet(ws, sections):
    row_idx = 1
    section_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    section_font = Font(color="1F4E78", bold=True)

    for section_name, rows in sections:
        ws.cell(row=row_idx, column=1, value=section_name)
        ws.cell(row=row_idx, column=1).fill = section_fill
        ws.cell(row=row_idx, column=1).font = section_font
        row_idx += 1

        headers = list(rows[0].keys()) if rows else ["note"]
        for col_idx, header in enumerate(headers, start=1):
            ws.cell(row=row_idx, column=col_idx, value=header)
        for cell in ws[row_idx]:
            cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
            cell.font = Font(color="FFFFFF", bold=True)
            cell.alignment = Alignment(horizontal="left", vertical="center")
        row_idx += 1

        if rows:
            for row in rows:
                for col_idx, header in enumerate(headers, start=1):
                    ws.cell(row=row_idx, column=col_idx, value=_cell_value(row.get(header, "")))
                row_idx += 1
        else:
            ws.cell(row=row_idx, column=1, value="No rows")
            row_idx += 1

        row_idx += 1

    for col in ws.columns:
        col_letter = col[0].column_letter
        max_len = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
        ws.column_dimensions[col_letter].width = min(max(12, max_len + 2), 80)


def _write_workbook(sheet_rows, output_path):
    wb = Workbook()
    first = True
    for sheet_name, rows in sheet_rows.items():
        if first:
            ws = wb.active
            ws.title = sheet_name
            first = False
        else:
            ws = wb.create_sheet(title=sheet_name)
        if isinstance(rows, dict) and isinstance(rows.get("sections"), list):
            _write_multi_section_sheet(ws, rows["sections"])
        else:
            sections = _split_nested_sections(sheet_name, rows)
            if len(sections) == 1 and sections[0][0] == "Data":
                _write_sheet(ws, sections[0][1])
            else:
                _write_multi_section_sheet(ws, sections)
    wb.save(output_path)


def main():
    parser = argparse.ArgumentParser(description="Convert schema JSON to styled Excel workbook")
    parser.add_argument("input_json", help="Path to source JSON file")
    parser.add_argument("output_xlsx", nargs="?", help="Path to output .xlsx file")
    args = parser.parse_args()

    input_path = Path(args.input_json).expanduser().resolve()
    output_path = (
        Path(args.output_xlsx).expanduser().resolve()
        if args.output_xlsx
        else input_path.with_suffix(".xlsx")
    )

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    sheet_rows = _collect_sheets(payload)
    _write_workbook(sheet_rows, output_path)
    row_count = sum(len(v) for v in sheet_rows.values())
    print(f"Wrote {row_count} rows across {len(sheet_rows)} sheets to {output_path}")


if __name__ == "__main__":
    main()
