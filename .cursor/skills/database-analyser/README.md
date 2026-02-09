# Database Analyzer Skill for Cursor

A Cursor skill that analyzes database schemas and produces fully enriched JSON files with comprehensive metadata.

## Features

- **Complete schema analysis**: Tables, columns, primary keys, foreign keys
- **Enriched metadata**: Row counts, field classifications, sensitive fields detection
- **ETL support**: Identifies incremental columns, partition columns, CDC status
- **Multi-database support**: Works with PostgreSQL (easily extensible to other databases)

## Installation

Copy the entire `.cursor/skills/` folder to your project:

```bash
# Copy skills folder to your project (project-level)
cp -r .cursor/skills /path/to/your/project/.cursor/

# Or copy to your home directory for global access (available across all projects)
cp -r .cursor/skills ~/.cursor/
```

## Prerequisites

1. **Python 3.7+** (includes `venv` module)

2. **Set up a virtual environment** (recommended):

```bash
# Create virtual environment
python3 -m venv .venv

# Activate it (Linux/Mac)
source .venv/bin/activate

# Or activate it (Windows)
.venv\Scripts\activate
```

3. **Install required Python packages**:

```bash
pip install sqlalchemy psycopg2-binary
```

## Inputs

The database analyzer requires the following inputs:

### Required

- **Database connection string**: Connection URL in format `postgresql://user:password@host:port/database`
  - Example: `postgresql://postgres:mypassword@localhost:5432/mydb`
  - Can be provided via:
    - Direct parameter when running the script
    - Environment variable: `DATABASE_URL`, `DB_URL`, `POSTGRES_URL`, or `DB_CONNECTION_STRING`
    - Configuration files (`.env`, `config.py`, etc.)

- **Output JSON path**: Path where the enriched schema JSON file will be saved
  - Example: `schema.json` or `output/schema.json`

### Optional

- **Schema name**: Specific schema to analyze (defaults to analyzing all schemas)
  - Example: `public`, `sales`, `analytics`
  - If not provided, analyzes all schemas in the database

## Usage

The skill is automatically available when you:
- Ask to analyze a database schema
- Request schema documentation
- Need to understand database structure
- Plan data migrations or ETL pipelines

### Manual Script Usage

You can also run the analyzer script directly:

```bash
python .cursor/skills/database-analyser/scripts/database_analyzer.py \
  "postgresql://user:pass@host/db" \
  output.json \
  public
```

## Output Format

The analyzer produces a JSON file with:
- Connection metadata (host, port, database, timezone)
- Per-table metadata (columns, PKs, FKs, row counts)
- Field classifications (pricing, quantity, categorical, temporal, contact)
- Sensitive fields detection (PII, financial, credentials)
- Incremental columns (for ETL watermarking)
- Partition columns (for date partitioning)
- CDC status (change data capture configuration)

## Example

```bash
# Analyze PostgreSQL database
python .cursor/skills/database-analyser/scripts/database_analyzer.py \
  "postgresql://postgres:password@localhost/mydb" \
  schema.json \
  public
```

## Requirements

- Python 3.7+ (includes venv module)
- Virtual environment (recommended)
- SQLAlchemy
- psycopg2-binary (for PostgreSQL)

