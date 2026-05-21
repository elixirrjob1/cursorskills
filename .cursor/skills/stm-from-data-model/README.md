# STM From Data Model (v2)

Generate one source-to-target mapping (STM) markdown file per target table using:
- a target data model markdown file
- a source system analysis schema JSON file

## Default folders

- Model input: `output/modeling`
- Schema JSON input: `output/source-system-analysis`
- STM output: `output/stm`

## Field-level constraints

Optional extended column table in the target model (per `### Table` section):

`| Column | Data Type | Nullable | Constraints | Description |`

Populated **Constraints** cells appear in STM Section 7. Table-level `**Business Key**:` and `**Foreign Keys**:` blocks are echoed after the Section 4 target table row.

## Environment configuration (`.env`)

The generator loads `.env` from the current working directory, then from repo root.
If no `OPENMETADATA_*` variables are present after that, it falls back to `OpenMetadata.env` (cwd first, then repo root).

Required warehouse context variables (set one from each group):
- Source system: `STM_SOURCE_SYSTEM` or `SOURCE_SYSTEM`
- Source database/schema: `STM_SOURCE_DATABASE_SCHEMA` or `SOURCE_DATABASE_SCHEMA`
- Target database: `STM_TARGET_DATABASE` or `TARGET_DATABASE` or `SNOWFLAKE_DATABASE`
- Target schema: `STM_TARGET_SCHEMA` or `TARGET_SCHEMA` or `SNOWFLAKE_SCHEMA`

Required OpenMetadata variables:
- `OPENMETADATA_BASE_URL`
- `OM_BASE_URL` and `OM_TOKEN` (legacy aliases: `OPENMETADATA_BASE_URL`, `OPENMETADATA_JWT_TOKEN`)

## Usage

### Use defaults

```bash
python3 .cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py
```

### Override paths

```bash
python3 .cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py \
  --input output/modeling/<model>.md \
  --analyzer-json output/source-system-analysis/<schema>.json \
  --output-dir output/stm
```

## Post-generate cleanup

After each run, the script removes stale STM files in the output directory left over from previous runs (renamed tables, reordered indexes, dropped tables).

- Only files matching `<index>-<TableName>-stm.md` are eligible for removal.
- The STM files written by the current run are always kept.
- `README.md` and any unrelated files in the output directory are not touched.
- Removed filenames are printed at the end of the run for auditability.
