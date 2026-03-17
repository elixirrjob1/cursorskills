#!/usr/bin/env python3
"""Enable Change Tracking on prediction.collection_runs for Fivetran.
Loads AZURE_MSSQL_URL from Key Vault (AZURE-MSSQL-URL) or .env. Requires KEYVAULT_NAME in .env."""
import os
import sys
from pathlib import Path

# Ensure project root on path for keyvault_loader
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.keyvault_loader import load_env

from sqlalchemy import create_engine, text

load_env()

DB_URL = os.environ.get("AZURE_MSSQL_URL")
if not DB_URL:
    print("Set KEYVAULT_NAME in .env and ensure AZURE-MSSQL-URL exists in Key Vault, or set AZURE_MSSQL_URL in .env.")
    exit(1)
if not DB_URL.startswith("mssql"):
    DB_URL = f"mssql+pyodbc://{DB_URL}"

def main():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE [prediction].[collection_runs] ENABLE CHANGE_TRACKING"))
        conn.commit()
    print("Change Tracking enabled on prediction.collection_runs")

if __name__ == "__main__":
    main()
