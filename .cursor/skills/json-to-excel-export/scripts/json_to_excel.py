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


def _compact_json(value):
    if value in (None, "", [], {}):
        return ""
    try:
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    except Exception:
        return _cell_value(value)


def _contacts_rows(source_system_context):
    contacts = source_system_context.get("contacts", [])
    rows = []
    if isinstance(contacts, list):
        for c in contacts:
            if isinstance(c, dict):
                rows.append(
                    {
                        "contact_name": c.get("name", ""),
                        "role": c.get("role", ""),
                        "email": c.get("email", ""),
                        "phone": c.get("phone", ""),
                        "notes": c.get("notes", ""),
                    }
                )
            else:
                rows.append(
                    {
                        "contact_name": _cell_value(c),
                        "role": "",
                        "email": "",
                        "phone": "",
                        "notes": "",
                    }
                )
    # Provide blank fillable lines for manual completion.
    while len(rows) < 8:
        rows.append({"contact_name": "", "role": "", "email": "", "phone": "", "notes": ""})
    return rows


def _delete_management_rows(source_system_context):
    instruction = source_system_context.get("delete_management_instruction", "")
    rows = [
        {
            "table_name": "",
            "delete_strategy": "",
            "instruction": _cell_value(instruction),
            "notes": "",
        }
    ]
    while len(rows) < 8:
        rows.append({"table_name": "", "delete_strategy": "", "instruction": "", "notes": ""})
    return rows


def _restrictions_rows(source_system_context):
    restrictions = source_system_context.get("restrictions", "")
    rows = []
    if isinstance(restrictions, list):
        for r in restrictions:
            if isinstance(r, dict):
                rows.append(
                    {
                        "restriction_type": r.get("type", ""),
                        "scope": r.get("scope", ""),
                        "details": r.get("details", ""),
                        "owner": r.get("owner", ""),
                    }
                )
            else:
                rows.append(
                    {
                        "restriction_type": "",
                        "scope": "",
                        "details": _cell_value(r),
                        "owner": "",
                    }
                )
    elif isinstance(restrictions, dict):
        rows.append(
            {
                "restriction_type": restrictions.get("type", ""),
                "scope": restrictions.get("scope", ""),
                "details": restrictions.get("details", ""),
                "owner": restrictions.get("owner", ""),
            }
        )
    else:
        rows.append({"restriction_type": "", "scope": "", "details": _cell_value(restrictions), "owner": ""})

    while len(rows) < 8:
        rows.append({"restriction_type": "", "scope": "", "details": "", "owner": ""})
    return rows


def _late_arriving_manual_rows(source_system_context):
    rows = [
        {
            "table_name": "",
            "business_date_column": "",
            "system_ts_column": "",
            "lookback_days": "",
            "policy_notes": _cell_value(source_system_context.get("late_arriving_data_manual", "")),
        }
    ]
    while len(rows) < 8:
        rows.append(
            {
                "table_name": "",
                "business_date_column": "",
                "system_ts_column": "",
                "lookback_days": "",
                "policy_notes": "",
            }
        )
    return rows


def _volume_projection_manual_rows(source_system_context):
    rows = [
        {
            "entity_scope": "",
            "projection_horizon_months": "",
            "growth_assumption_pct": "",
            "basis": "",
            "notes": _cell_value(source_system_context.get("volume_size_projection_manual", "")),
        }
    ]
    while len(rows) < 8:
        rows.append(
            {
                "entity_scope": "",
                "projection_horizon_months": "",
                "growth_assumption_pct": "",
                "basis": "",
                "notes": "",
            }
        )
    return rows


def _field_context_manual_rows(source_system_context):
    rows = [
        {
            "table_name": "",
            "column_name": "",
            "business_context": _cell_value(source_system_context.get("field_context_manual", "")),
            "transformation_notes": "",
            "owner": "",
        }
    ]
    while len(rows) < 8:
        rows.append(
            {
                "table_name": "",
                "column_name": "",
                "business_context": "",
                "transformation_notes": "",
                "owner": "",
            }
        )
    return rows


def _row_from_finding(schema_name, table_name, idx, finding):
    if not isinstance(finding, dict):
        return {
            "schema": schema_name,
            "table_name": table_name,
            "finding_index": idx,
            "check": "",
            "severity": "",
            "column": "",
            "detail": _cell_value(finding),
            "recommendation": "",
            "distinct_values": "",
            "suggested_domain": "",
            "sample_values": "",
            "cardinality": "",
            "delete_strategy": "",
            "soft_delete_column": "",
            "soft_delete_type": "",
            "has_audit_trail": "",
            "business_date_column": "",
            "system_ts_column": "",
            "server_timezone": "",
            "timezone_columns": "",
            "distinct_timezones": "",
            "tz_aware_count": "",
            "tz_naive_count": "",
            "detected_unit": "",
            "canonical_unit": "",
            "extra_json": "",
        }

    lag_stats = finding.get("lag_stats") or {}
    if not isinstance(lag_stats, dict):
        lag_stats = {}

    known_keys = {
        "check", "severity", "column", "detail", "recommendation",
        "distinct_values", "suggested_domain", "sample_values", "cardinality",
        "delete_strategy", "soft_delete_column", "soft_delete_type", "has_audit_trail",
        "business_date_column", "system_ts_column", "recommended_lookback_days", "lag_stats",
        "server_timezone", "columns", "distinct_timezones", "tz_aware_count", "tz_naive_count",
        "detected_unit", "canonical_unit",
    }
    extra_fields = {k: v for k, v in finding.items() if k not in known_keys}

    return {
        "schema": schema_name,
        "table_name": table_name,
        "finding_index": idx,
        "check": finding.get("check", ""),
        "severity": finding.get("severity", ""),
        "column": finding.get("column", ""),
        "detail": finding.get("detail", ""),
        "recommendation": finding.get("recommendation", ""),
        "distinct_values": _cell_value(finding.get("distinct_values", "")),
        "suggested_domain": _cell_value(finding.get("suggested_domain", "")),
        "sample_values": _cell_value(finding.get("sample_values", "")),
        "cardinality": finding.get("cardinality", ""),
        "delete_strategy": finding.get("delete_strategy", ""),
        "soft_delete_column": finding.get("soft_delete_column", ""),
        "soft_delete_type": finding.get("soft_delete_type", ""),
        "has_audit_trail": _cell_value(finding.get("has_audit_trail", "")),
        "business_date_column": finding.get("business_date_column", ""),
        "system_ts_column": finding.get("system_ts_column", ""),
        "server_timezone": finding.get("server_timezone", ""),
        "timezone_columns": _compact_json(finding.get("columns")),
        "distinct_timezones": _cell_value(finding.get("distinct_timezones", "")),
        "tz_aware_count": finding.get("tz_aware_count", ""),
        "tz_naive_count": finding.get("tz_naive_count", ""),
        "detected_unit": finding.get("detected_unit", ""),
        "canonical_unit": finding.get("canonical_unit", ""),
        "extra_json": _compact_json(extra_fields),
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
    source_system_context = payload.get("source_system_context", {}) or {}
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
        {"metric": "port", "value": connection.get("port", "")},
        {"metric": "database_timezone", "value": connection.get("timezone", "")},
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
    data_quality_findings_rows = []
    data_quality_lag_rows = []

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
                "partition_columns_candidates": _join_list(table.get("partition_columns_candidates", [])),
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

        findings = ((table.get("data_quality") or {}).get("findings") or [])
        for idx, finding in enumerate(findings, start=1):
            data_quality_findings_rows.append(
                _row_from_finding(schema_name, table_name, idx, finding)
            )
            if isinstance(finding, dict) and finding.get("check") == "late_arriving_data":
                lag_stats = finding.get("lag_stats") or {}
                if not isinstance(lag_stats, dict):
                    lag_stats = {}
                data_quality_lag_rows.append(
                    {
                        "schema": schema_name,
                        "table_name": table_name,
                        "finding_index": idx,
                        "severity": finding.get("severity", ""),
                        "business_date_column": finding.get("business_date_column", ""),
                        "system_ts_column": finding.get("system_ts_column", ""),
                        "recommended_lookback_days": finding.get("recommended_lookback_days", ""),
                        "lag_total_rows_compared": lag_stats.get("total_rows_compared", ""),
                        "lag_min_lag_hours": lag_stats.get("min_lag_hours", ""),
                        "lag_avg_lag_hours": lag_stats.get("avg_lag_hours", ""),
                        "lag_p95_lag_hours": lag_stats.get("p95_lag_hours", ""),
                        "lag_max_lag_hours": lag_stats.get("max_lag_hours", ""),
                        "lag_max_lag_days": lag_stats.get("max_lag_days", ""),
                        "lag_rows_late_over_1d": lag_stats.get("rows_late_over_1d", ""),
                        "lag_rows_late_over_7d": lag_stats.get("rows_late_over_7d", ""),
                        "detail": finding.get("detail", ""),
                        "recommendation": finding.get("recommendation", ""),
                    }
                )

    summary_sections = [("Overview", summary)]
    if dq_by_check_rows:
        summary_sections.append(("DataQualityByCheck", dq_by_check_rows))
    if dq_constraints_rows:
        summary_sections.append(("DataQualityConstraints", dq_constraints_rows))
    if dq_other_rows:
        summary_sections.append(("DataQualityDetails", dq_other_rows))

    source_context_sections = [
        ("ContactsManual", _contacts_rows(source_system_context)),
        ("DeleteManagementManual", _delete_management_rows(source_system_context)),
        ("RestrictionsManual", _restrictions_rows(source_system_context)),
        ("LateArrivingDataManual", _late_arriving_manual_rows(source_system_context)),
        ("VolumeSizeProjectionManual", _volume_projection_manual_rows(source_system_context)),
        ("FieldContextManual", _field_context_manual_rows(source_system_context)),
    ]

    sheets = {
        "Summary": {"sections": summary_sections},
        "SourceContextManual": {"sections": source_context_sections},
        "DataQualityFindings": data_quality_findings_rows,
        "latearivingdata": data_quality_lag_rows,
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
    if ws.title == "DataQualityFindings":
        wrap_cols = {"detail", "recommendation", "timezone_columns", "extra_json"}
        for idx, header in enumerate(headers, start=1):
            if header in wrap_cols:
                col_letter = ws.cell(row=1, column=idx).column_letter
                ws.column_dimensions[col_letter].width = 60
                for r in range(2, ws.max_row + 1):
                    cell = ws.cell(row=r, column=idx)
                    cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)


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
    if ws.title == "SourceContextManual":
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
            for cell in row:
                cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
        # Wider manual input fields for easier data entry.
        ws.column_dimensions["A"].width = 24
        ws.column_dimensions["B"].width = 24
        ws.column_dimensions["C"].width = 64
        ws.column_dimensions["D"].width = 28
        ws.column_dimensions["E"].width = 36


def _write_workbook(sheet_rows, output_path):
    wb = Workbook()
    first = True
    for sheet_name, rows in sheet_rows.items():
        if isinstance(rows, dict) and isinstance(rows.get("sections"), list):
            has_data = any(section_rows for _, section_rows in rows["sections"])
            if not has_data:
                continue
        elif isinstance(rows, list) and not rows:
            continue

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
    if first:
        ws = wb.active
        ws.title = "Summary"
        _write_sheet(ws, [])
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
