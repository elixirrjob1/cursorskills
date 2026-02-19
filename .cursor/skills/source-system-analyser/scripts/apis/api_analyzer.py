#!/usr/bin/env python3
"""
Analyze API data files and generate normalized schema.json output.

Reads JSON files downloaded by api_reader.py and produces schema.json
following the shared output schema contract.
"""

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Type inference helpers
def infer_type(value: Any) -> str:
    """Infer SQL-like type from Python value."""
    if value is None:
        return "text"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "numeric"
    if isinstance(value, str):
        # Check for common patterns
        if re.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?", value):
            return "timestamp"
        if re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", value):
            return "text"  # email, but keep as text
        if re.match(r"^\+?\d[\d\s\-\(\)]+$", value):
            return "text"  # phone, but keep as text
        return "text"
    return "text"


def analyze_api_data(discovery_file: Path, data_dir: Path, base_url: str) -> Dict[str, Any]:
    """Analyze API data and generate normalized schema.json."""
    
    # Load discovery data
    with open(discovery_file, "r", encoding="utf-8") as f:
        discovery = json.load(f)
    
    tables_list = discovery.get("tables", [])
    schema_name = "dbo"  # Default from test API
    
    # Process each table
    tables = []
    all_findings = []
    
    for table_name in tables_list:
        data_file = data_dir / table_name
        if not data_file.exists():
            continue
        
        with open(data_file, "r", encoding="utf-8") as f:
            table_data = json.load(f)
        
        schema = table_data.get("schema", schema_name)
        records = table_data.get("data", [])
        
        if not records:
            continue
        
        # Infer columns from first record
        first_record = records[0]
        columns = []
        column_types = {}
        null_counts = Counter()
        value_samples = {}  # For controlled value detection
        
        for col_name, col_value in first_record.items():
            col_type = infer_type(col_value)
            columns.append({
                "name": col_name,
                "type": col_type,
                "nullable": True,  # Assume nullable unless proven otherwise
                "is_incremental": col_name in ("created_at", "updated_at", "id"),
                "cardinality": None,
                "null_count": 0,
                "data_range": {"min": None, "max": None},
                "data_category": None,
            })
            column_types[col_name] = col_type
            value_samples[col_name] = set()
        
        # Analyze all records
        for record in records:
            for col_name in columns:
                col_name_str = col_name["name"]
                value = record.get(col_name_str)
                
                if value is None:
                    null_counts[col_name_str] += 1
                else:
                    # Track unique values for cardinality
                    if isinstance(value, (str, int, float, bool)):
                        value_samples[col_name_str].add(str(value))
        
        # Update column metadata
        for col in columns:
            col_name = col["name"]
            col["null_count"] = null_counts[col_name]
            col["nullable"] = null_counts[col_name] > 0 or len(records) == 0
            unique_count = len(value_samples.get(col_name, set()))
            col["cardinality"] = unique_count if unique_count > 0 else None
            
            # Update data range for numeric types
            if col["type"] in ("integer", "numeric"):
                numeric_values = [
                    float(r.get(col_name, 0)) 
                    for r in records 
                    if r.get(col_name) is not None and isinstance(r.get(col_name), (int, float))
                ]
                if numeric_values:
                    col["data_range"]["min"] = str(min(numeric_values))
                    col["data_range"]["max"] = str(max(numeric_values))
        
        # Identify primary keys (fields ending in _id or just 'id')
        primary_keys = []
        for col in columns:
            col_name = col["name"]
            if col_name == "id" or (col_name.endswith("_id") and col_name != "id"):
                # Check if it's unique
                if col["cardinality"] == len(records):
                    primary_keys.append(col_name)
        
        # If no clear PK found, check for 'id' or '{table}_id'
        if not primary_keys:
            for col in columns:
                col_name = col["name"]
                if col_name == f"{table_name}_id" or col_name == "id":
                    primary_keys.append(col_name)
                    break
        
        # Identify foreign keys (fields ending in _id that reference other tables)
        foreign_keys = []
        for col in columns:
            col_name = col["name"]
            if col_name.endswith("_id") and col_name not in primary_keys:
                # Infer referenced table from column name
                ref_table = col_name.replace("_id", "")
                if ref_table in tables_list:
                    foreign_keys.append({
                        "column": col_name,
                        "references": f"{schema}.{ref_table}({ref_table}_id)"
                    })
        
        # Data quality checks
        findings = []
        
        # Check for missing primary key
        if not primary_keys:
            findings.append({
                "severity": "warning",
                "check": "missing_primary_key",
                "table": table_name,
                "message": f"Table '{table_name}' has no identified primary key",
            })
        
        # Check for controlled value candidates (low cardinality)
        for col in columns:
            if col["cardinality"] and col["cardinality"] <= 10 and col["cardinality"] > 0:
                if col["cardinality"] < len(records) * 0.1:  # Less than 10% unique
                    findings.append({
                        "severity": "info",
                        "check": "controlled_value_candidates",
                        "table": table_name,
                        "column": col["name"],
                        "message": f"Column '{col['name']}' has low cardinality ({col['cardinality']} distinct values), may be a controlled value",
                    })
        
        # Check for nullable but never null
        for col in columns:
            if col["nullable"] and col["null_count"] == 0 and len(records) > 0:
                findings.append({
                    "severity": "info",
                    "check": "nullable_but_never_null",
                    "table": table_name,
                    "column": col["name"],
                    "message": f"Column '{col['name']}' is nullable but contains no null values in sample",
                })
        
        # Check for format inconsistencies (emails, phones)
        for col in columns:
            col_name = col["name"]
            if "email" in col_name.lower():
                email_values = [
                    str(r.get(col_name, "")) 
                    for r in records 
                    if r.get(col_name) is not None
                ]
                invalid_emails = [
                    e for e in email_values 
                    if e and not re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", e)
                ]
                if invalid_emails:
                    findings.append({
                        "severity": "warning",
                        "check": "format_inconsistency",
                        "table": table_name,
                        "column": col_name,
                        "message": f"Column '{col_name}' contains {len(invalid_emails)} invalid email format(s)",
                    })
            
            if "phone" in col_name.lower():
                phone_values = [
                    str(r.get(col_name, "")) 
                    for r in records 
                    if r.get(col_name) is not None
                ]
                # Basic phone validation
                invalid_phones = [
                    p for p in phone_values 
                    if p and not re.match(r"^[\+\d\s\-\(\)]+$", p)
                ]
                if invalid_phones:
                    findings.append({
                        "severity": "info",
                        "check": "format_inconsistency",
                        "table": table_name,
                        "column": col_name,
                        "message": f"Column '{col_name}' contains {len(invalid_phones)} potentially invalid phone format(s)",
                    })
        
        # Check for delete management (soft delete flags)
        delete_flags = [col["name"] for col in columns if "deleted" in col["name"].lower() or "active" in col["name"].lower()]
        if not delete_flags:
            findings.append({
                "severity": "info",
                "check": "delete_management",
                "table": table_name,
                "message": f"Table '{table_name}' has no identified soft delete flag",
            })
        
        # Check for late arriving data (timestamp columns)
        timestamp_cols = [col["name"] for col in columns if col["type"] == "timestamp"]
        if timestamp_cols:
            findings.append({
                "severity": "info",
                "check": "late_arriving_data",
                "table": table_name,
                "message": f"Table '{table_name}' has timestamp columns: {', '.join(timestamp_cols)}. Monitor for late-arriving data.",
            })
        
        table_entry = {
            "table": table_name,
            "schema": schema,
            "columns": columns,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "row_count": len(records),
            "data_quality": {
                "controlled_value_candidates": [
                    f for f in findings if f["check"] == "controlled_value_candidates"
                ],
                "nullable_but_never_null": [
                    f for f in findings if f["check"] == "nullable_but_never_null"
                ],
                "missing_primary_key": [
                    f for f in findings if f["check"] == "missing_primary_key"
                ],
                "missing_foreign_keys": [],
                "format_inconsistency": [
                    f for f in findings if f["check"] == "format_inconsistency"
                ],
                "range_violations": [],
                "delete_management": [
                    f for f in findings if f["check"] == "delete_management"
                ],
                "late_arriving_data": [
                    f for f in findings if f["check"] == "late_arriving_data"
                ],
                "timezone": [],
                "findings": findings,
            },
        }
        
        tables.append(table_entry)
        all_findings.extend(findings)
    
    # Calculate summary statistics
    severity_counts = Counter(f["severity"] for f in all_findings)
    check_counts = Counter(f["check"] for f in all_findings)
    
    total_rows = sum(t.get("row_count", 0) for t in tables)
    
    schema_document = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database_url": base_url,
            "schema_filter": schema_name,
            "total_tables": len(tables),
            "total_rows": total_rows,
            "total_findings": len(all_findings),
        },
        "connection": {
            "host": None,
            "port": None,
            "database": None,
            "driver": "rest_api",
            "timezone": None,
        },
        "data_quality_summary": {
            "critical": severity_counts.get("critical", 0),
            "warning": severity_counts.get("warning", 0),
            "info": severity_counts.get("info", 0),
            "by_check": dict(check_counts),
            "constraints_found": {},
        },
        "tables": tables,
    }
    
    return schema_document


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze API data and generate schema.json")
    parser.add_argument("--discovery", required=True, help="Path to API discovery JSON file")
    parser.add_argument("--data-dir", required=True, help="Directory containing downloaded API data files")
    parser.add_argument("--base-url", required=True, help="Base URL of the API")
    parser.add_argument("--output", required=True, help="Output schema.json file path")
    args = parser.parse_args()
    
    discovery_file = Path(args.discovery)
    data_dir = Path(args.data_dir)
    output_file = Path(args.output)
    
    if not discovery_file.exists():
        print(f"Error: Discovery file not found: {discovery_file}")
        return 1
    
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        return 1
    
    schema_document = analyze_api_data(discovery_file, data_dir, args.base_url)
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(schema_document, f, indent=2, default=str)
    
    print(f"Generated schema.json: {output_file}")
    print(f"  Tables: {len(schema_document['tables'])}")
    print(f"  Total rows: {schema_document['metadata']['total_rows']}")
    print(f"  Findings: {schema_document['metadata']['total_findings']}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
