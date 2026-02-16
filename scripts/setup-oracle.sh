#!/bin/bash
# Start local Oracle XE via Docker and prepare for schema mirroring.
# Run: ./scripts/setup-oracle.sh
# Requires: docker, docker compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "Starting Oracle XE container..."
docker compose up -d oracle

echo ""
echo "Waiting for Oracle to be ready (first startup can take 2-3 minutes)..."
echo "Check status: docker compose logs -f oracle"
echo ""
echo "When ready, add to .env:"
echo "  ORACLE_URL=oracle+oracledb://app:AppPassword1@localhost:1521/?service_name=XEPDB1"
echo ""
echo "Or with cx_Oracle:"
echo "  ORACLE_URL=oracle+cx_oracle://app:AppPassword1@localhost:1521/?service_name=XEPDB1"
echo ""
echo "Then: pip install oracledb  # or: pip install cx_Oracle"
echo "Then: python scripts/schema_json_to_oracle.py"
echo "Then: .venv/bin/python .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \"\$ORACLE_URL\" schema.json app --dialect oracle"
echo ""
echo "Default passwords (from docker-compose): ORACLE_PASSWORD=OraclePassword1, APP user=AppPassword1"
