#!/bin/bash
# Reset SA password for local SQL Server on Linux.
# Run: ./scripts/setup-mssql.sh
# You will be prompted for your sudo password and the new SA password.

set -e

echo "Stopping mssql-server..."
sudo systemctl stop mssql-server

echo "Setting SA password (you will be prompted for a new password)..."
echo "Password must be 8+ chars with uppercase, lowercase, digits, and symbols."
sudo /opt/mssql/bin/mssql-conf set-sa-password

echo "Starting mssql-server..."
sudo systemctl start mssql-server

echo "Done. Add to .env:"
echo "  MSSQL_URL=mssql+pyodbc://sa:YOUR_PASSWORD@localhost:1433/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
echo ""
echo "Then: pip install pyodbc"
echo "Then: .venv/bin/python .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \"\$MSSQL_URL\" schema.json --dialect mssql"
