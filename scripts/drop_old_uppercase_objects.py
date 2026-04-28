#!/usr/bin/env python3
"""
Drop old uppercase Snowflake views/tables created before identifier quoting was enabled.
Run once to clear the namespace clash, then re-run dbt with quoting: identifier: true.
"""
import os
import sys
from pathlib import Path

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import snowflake.connector

account  = os.environ["SNOWFLAKE_ACCOUNT"]
user     = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_FIVETRAN_PASSWORD"]
database = os.environ.get("SNOWFLAKE_DATABASE", "DRIP_DATA_INTELLIGENCE")
warehouse= os.environ.get("SNOWFLAKE_WAREHOUSE", "FIVETRAN_DRIP_WH")

# Schema names dbt uses with the default generate_schema_name macro
DEV_SCHEMA      = "DBT_DEV"
ENRICHED_SCHEMA = "DBT_DEV_ENRICHED"

VIEWS_TO_DROP = [
    "VW_DIMCUSTOMER",
    "VW_DIMDATE",
    "VW_DIMEMPLOYEE",
    "VW_DIMPRODUCT",
    "VW_DIMSTORE",
    "VW_DIMSUPPLIER",
    "VW_DIMWAREHOUSE",
    "VW_FACTINVENTORYSNAPSHOT",
    "VW_FACTSALES",
    "VW_FACTPURCHASEORDER",
]

TABLES_TO_DROP = [
    "DIMCUSTOMER",
    "DIMDATE",
    "DIMEMPLOYEE",
    "DIMPRODUCT",
    "DIMSTORE",
    "DIMSUPPLIER",
    "DIMWAREHOUSE",
    "FACTINVENTORYSNAPSHOT",
    "FACTSALES",
    "FACTPURCHASEORDER",
]

con = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    database=database,
    warehouse=warehouse,
)

cur = con.cursor()
errors = []

print(f"Connected to {database}")

for v in VIEWS_TO_DROP:
    sql = f'DROP VIEW IF EXISTS {database}.{DEV_SCHEMA}."{v}"'
    try:
        cur.execute(sql)
        print(f"  DROP VIEW  {DEV_SCHEMA}.{v}")
    except Exception as e:
        print(f"  WARN view {v}: {e}", file=sys.stderr)
        errors.append(str(e))

for t in TABLES_TO_DROP:
    # Try both the dev schema and the enriched schema
    for schema in (DEV_SCHEMA, ENRICHED_SCHEMA):
        sql = f'DROP TABLE IF EXISTS {database}.{schema}."{t}"'
        try:
            cur.execute(sql)
            print(f"  DROP TABLE {schema}.{t}")
        except Exception as e:
            print(f"  WARN table {schema}.{t}: {e}", file=sys.stderr)
            errors.append(str(e))

cur.close()
con.close()

if errors:
    print(f"\n{len(errors)} warning(s) — see above.", file=sys.stderr)
else:
    print("\nAll done — no errors.")

sys.exit(0)
