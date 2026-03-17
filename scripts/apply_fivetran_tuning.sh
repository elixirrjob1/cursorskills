#!/usr/bin/env bash
# Apply Fivetran tuning recommendations from schema analysis
# This script hashes PII fields and applies optimal configurations

set -euo pipefail

CONNECTOR_ID="${FIVETRAN_CONNECTOR_ID:-insatiable_cyst}"
SCHEMA_NAME="${FIVETRAN_SCHEMA_NAME:-dbo}"

echo "Applying Fivetran tuning recommendations..."
echo "Connector: $CONNECTOR_ID"
echo "Schema: $SCHEMA_NAME"
echo ""

# Hash email columns
echo "Hashing email columns..."
python3 <<EOF
import sys
sys.path.insert(0, 'tools/fivetran_mcp')
from server import update_column_config

tables_columns = [
    ("customers", "email"),
    ("employees", "email"),
    ("suppliers", "email"),
]

for table, column in tables_columns:
    try:
        result = update_column_config(
            connector_id="$CONNECTOR_ID",
            schema_name="$SCHEMA_NAME",
            table_name=table,
            column_name=column,
            hashed=True
        )
        print(f"✅ Hashed {table}.{column}")
    except Exception as e:
        print(f"❌ Failed to hash {table}.{column}: {e}")
EOF

# Hash phone columns
echo ""
echo "Hashing phone columns..."
python3 <<EOF
import sys
sys.path.insert(0, 'tools/fivetran_mcp')
from server import update_column_config

tables_columns = [
    ("customers", "phone"),
    ("stores", "phone"),
    ("suppliers", "phone"),
]

for table, column in tables_columns:
    try:
        result = update_column_config(
            connector_id="$CONNECTOR_ID",
            schema_name="$SCHEMA_NAME",
            table_name=table,
            column_name=column,
            hashed=True
        )
        print(f"✅ Hashed {table}.{column}")
    except Exception as e:
        print(f"❌ Failed to hash {table}.{column}: {e}")
EOF

# Hash address fields
echo ""
echo "Hashing address fields..."
python3 <<EOF
import sys
sys.path.insert(0, 'tools/fivetran_mcp')
from server import update_column_config

tables_columns = [
    ("stores", "address"),
    ("stores", "postal_code"),
]

for table, column in tables_columns:
    try:
        result = update_column_config(
            connector_id="$CONNECTOR_ID",
            schema_name="$SCHEMA_NAME",
            table_name=table,
            column_name=column,
            hashed=True
        )
        print(f"✅ Hashed {table}.{column}")
    except Exception as e:
        print(f"❌ Failed to hash {table}.{column}: {e}")
EOF

echo ""
echo "✅ Fivetran tuning recommendations applied!"
echo ""
echo "Note: Hashed columns will show hashed values in destination."
echo "Original values are preserved in source database."
