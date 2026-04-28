#!/usr/bin/env python3
"""
Parse OpenMetadata get_table payloads and build a metadata backup JSON.
Maps old UPPERCASE table/column names to their new PascalCase equivalents.
Output: scripts/om_metadata_backup.json
"""
import json
from pathlib import Path

# Old uppercase → new PascalCase table name mapping
TABLE_MAP = {
    "DIMDATE":                "DimDate",
    "DIMEMPLOYEE":            "DimEmployee",
    "DIMSTORE":               "DimStore",
    "DIMSUPPLIER":            "DimSupplier",
    "DIMWAREHOUSE":           "DimWarehouse",
    "DIMPRODUCT":             "DimProduct",
    "DIMCUSTOMER":            "DimCustomer",
    "FACTSALES":              "FactSales",
    "FACTINVENTORYSNAPSHOT":  "FactInventorySnapshot",
    "FACTPURCHASEORDER":      "FactPurchaseOrder",
}

SCHEMA     = "DBT_PROD_ENRICHED"
DATABASE   = "DRIP_DATA_INTELLIGENCE"
SERVICE    = "snowflake_fivetran"

# The 10 raw payload files — order matches TABLE_MAP keys above
PAYLOAD_FILES = [
    "ab07d982-96dd-4ae3-bdbe-f702d84af343.txt",  # DIMDATE
    "7b9c3c32-1b1d-45bf-8e3c-a9ad286ecc9f.txt",  # DIMEMPLOYEE
    "545d2108-a9ea-4a9f-8b05-8665bcd91155.txt",  # DIMSTORE
    "5d5288e4-84fc-442d-8ea8-9b416e7bcd06.txt",  # DIMSUPPLIER
    "6f8c6e2b-10d4-464e-aa7d-9adc802c7c13.txt",  # DIMWAREHOUSE
    "5a198cd2-df6e-495d-8889-deabde66b7b6.txt",  # DIMPRODUCT
    "da82bf94-a7cd-4ce2-a8eb-b385bcb5a952.txt",  # DIMCUSTOMER
    "9d63733b-d9f4-40d6-8f9c-6faf77984cd6.txt",  # FACTSALES
    "0dbd0244-f17b-4df2-ac3e-f917f5a0f830.txt",  # FACTINVENTORYSNAPSHOT
    "8ffb28d2-282e-4dad-961a-5687afe12985.txt",  # FACTPURCHASEORDER
]

TOOLS_DIR = Path.home() / ".cursor/projects/home-fillip-projec-cursorskills/agent-tools"

def extract_tags(tag_list: list) -> list:
    """Return tag FQNs that are Classification type."""
    return [t["tagFQN"] for t in (tag_list or []) if t.get("source") == "Classification"]

def extract_glossary_terms(tag_list: list) -> list:
    """Return glossary term FQNs."""
    return [t["tagFQN"] for t in (tag_list or []) if t.get("source") == "Glossary"]

backup = {}

for old_name, new_name in TABLE_MAP.items():
    # find the matching payload file
    idx = list(TABLE_MAP.keys()).index(old_name)
    payload_path = TOOLS_DIR / PAYLOAD_FILES[idx]
    raw = json.loads(payload_path.read_text())

    table_data = raw.get("table", raw)  # payload wraps the table under "table" key

    table_tags      = extract_tags(table_data.get("tags", []))
    table_glossary  = extract_glossary_terms(table_data.get("tags", []))
    table_desc      = table_data.get("description", "")

    columns = {}
    for col in table_data.get("columns", []):
        old_col = col["name"]                       # uppercase, e.g. DATEHASHPK
        new_col = old_col                           # will be re-cased in apply script
        col_tags     = extract_tags(col.get("tags", []))
        col_glossary = extract_glossary_terms(col.get("tags", []))
        col_desc     = col.get("description", "")
        if col_tags or col_glossary or col_desc:
            columns[old_col] = {
                "tags":          col_tags,
                "glossary_terms": col_glossary,
                "description":   col_desc,
            }

    backup[old_name] = {
        "new_name":       new_name,
        "new_fqn":        f"{SERVICE}.{DATABASE}.{SCHEMA}.{new_name}",
        "description":    table_desc,
        "tags":           table_tags,
        "glossary_terms": table_glossary,
        "columns":        columns,
    }

    tag_count = len(table_tags) + len(table_glossary)
    col_count = sum(len(v["tags"]) + len(v["glossary_terms"]) for v in columns.values())
    print(f"  {old_name} → {new_name}: {tag_count} table tags, {col_count} column tags across {len(columns)} columns")

out = Path(__file__).parent / "om_metadata_backup.json"
out.write_text(json.dumps(backup, indent=2))
print(f"\nSaved → {out}")
