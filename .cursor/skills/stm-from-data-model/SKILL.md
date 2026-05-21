---
name: stm-from-data-model
description: Generate source-to-target mapping markdown documents from a target data-model markdown file plus analyzer schema JSON. Use when the user has a dimensional/star-schema model in markdown, an analyzer JSON with glossary and classification assignments, and wants one STM document per target table written to `output/stm`.
---

# STM From Data Model

## When to use

Use this skill when:
- the inputs are a markdown data model plus analyzer schema JSON
- the user wants STM/source-to-target mapping documents per target table
- the output should follow a fixed STM template
- analyzer glossary terms and classification tags should be copied into the STM
- unknown source-side values must remain blank, except for the warehouse source/target conventions populated from `.env` (see Population Rules)

## Inputs

- Default model folder: `output/modeling` (relative to project root)
- Default analyzer JSON folder: `output/source-system-analysis` (relative to project root)
- Default output folder: `output/stm` (relative to project root)
- Required inputs:
  - one markdown file describing the target warehouse model
  - one analyzer schema JSON file with table/column `glossary_terms` and `classification_tags`
- Environment requirements for glossary definitions:
  - Warehouse context from `.env`:
    - `STM_SOURCE_SYSTEM` (or `SOURCE_SYSTEM`)
    - `STM_SOURCE_DATABASE_SCHEMA` (or `SOURCE_DATABASE_SCHEMA`)
    - `STM_TARGET_DATABASE` (or `TARGET_DATABASE` / `SNOWFLAKE_DATABASE`)
    - `STM_TARGET_SCHEMA` (or `TARGET_SCHEMA` / `SNOWFLAKE_SCHEMA`)
  - `OM_BASE_URL` and `OM_TOKEN` (legacy aliases: `OPENMETADATA_BASE_URL`, `OPENMETADATA_JWT_TOKEN`)
  - OpenMetadata fallback file: if no `OM_*` / `OPENMETADATA_*` variables are set, load `OpenMetadata.env` (cwd first, then repo root)

If the caller provides explicit paths, use them. Otherwise:
- read the single `.md` file in `output/modeling`
- read the single `.json` file in `output/source-system-analysis`
- write all outputs to `output/stm`
- load warehouse and OpenMetadata connection values from `.env`

## Run

```bash
python3 .cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py \
  --input output/modeling/<model>.md \
  --analyzer-json output/source-system-analysis/<schema>.json \
  --output-dir output/stm
```

If there is exactly one markdown file in `output/modeling` and one analyzer JSON file in `output/source-system-analysis`, both path flags may be omitted:

```bash
python3 .cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py
```

## Output

The script generates:
- one STM markdown file per target table
- `README.md` in the output directory listing generated files
- separate STM sections for classification tags and glossary terms sourced from the analyzer JSON

File naming:
- `01-<TableName>-stm.md`
- `02-<TableName>-stm.md`
- etc.

### Post-generate cleanup

After writing the new STM files, the script removes stale STM files from previous runs in the output directory so reruns leave no duplicates behind (handles renamed tables, reordered indexes, or removed tables).

- Only files matching the `<index>-<TableName>-stm.md` pattern are eligible for removal.
- The freshly written files for the current run are always kept.
- `README.md` and any unrelated files in the output directory are left alone.
- The script prints the names of any removed files so the cleanup is auditable.

## Population Rules

Use warehouse values from `.env` in every generated STM:
- `Source System Inventory.Source System` comes from `STM_SOURCE_SYSTEM` (or `SOURCE_SYSTEM`)
- `Source System Inventory.Database / Schema` comes from `STM_SOURCE_DATABASE_SCHEMA` (or `SOURCE_DATABASE_SCHEMA`)
- `Target Schema Definition.Target Database` comes from `STM_TARGET_DATABASE` (or `TARGET_DATABASE` / `SNOWFLAKE_DATABASE`)
- `Target Schema Definition.Schema` comes from `STM_TARGET_SCHEMA` (or `TARGET_SCHEMA` / `SNOWFLAKE_SCHEMA`)
- `Source System Inventory.Table / File` remains `See field-level mapping`
- `Source System Inventory.Notes` remains `Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system.`

- Fill only fields derivable from the model markdown and analyzer JSON.
- Leave other unknown values literally blank.
- Do not invent source systems, source tables, source columns, owners, orchestration, DQ thresholds, glossary definitions, or classification tags.
- Do not query OpenMetadata during STM generation.
- Use analyzer JSON `classification_tags` to populate classification sections.
- Render only classification names and assigned tag FQNs in the classification section; do not expand classification definitions.
- Use analyzer JSON `glossary_terms` directly for glossary sections.
- Pull glossary definitions from the OpenMetadata API for the glossary terms referenced by the STM.
- If glossary terms are present and OpenMetadata API access is not configured, fail with a clear error rather than silently leaving definitions blank.
- If a referenced glossary term exists in the analyzer JSON but is not returned by OpenMetadata, fall back to the analyzer JSON definition when present; otherwise leave it blank.
- Fill transformation logic conservatively:
  - explicit formulas from the model
  - explicit SCD behavior
  - explicit grain statements
  - explicit measure notes

### Field-level constraints (optional column + table-level blocks)

When the target data model captures constraints, carry them into the STM automatically:

1. **Per-column constraints** — use an extended column table header (add **Constraints** between Nullable and Description):

   `| Column | Data Type | Nullable | Constraints | Description |`

   Put concise notes such as PK, FK references, UNIQUE, CHECK expressions, or “NOT NULL enforced in warehouse” in **Constraints**. Leave the cell blank when none apply.

   The legacy header without **Constraints** remains supported; those STMs still generate with an empty **Constraints** column in Section 7.

2. **Table-level keys** — keep using labeled blocks under each `### TableName` section:

   - `**Business Key**:` …
   - `**Foreign Keys**:` … (free text or bullet list)

   These are emitted under **Section 4. Target Schema Definition** immediately after the target table row so relationship rules survive generation.

### WarehouseHashFK — UNKNOWN fallback pattern

When a fact table target column is `WarehouseHashFK` and the source system provides no warehouse identifier, populate the STM's **Transformation / Business Rule** cell with:

```
HASH(COALESCE(CAST('UNKNOWN' AS VARCHAR), '#@#@#@#@#'))
```

and set the **Notes** cell to:

```
No warehouse granularity in source. Hardcoded to match DimWarehouse.WarehouseHashPK for the single UNKNOWN fallback record. Do NOT leave NULL — a NULL FK silently breaks all joins to DimWarehouse.
```

Do NOT leave Source Table / Source Column blank and Transformation blank in this case — that would cause the dbt model generator to emit `CAST(NULL AS NUMBER(19,0))`, which breaks the FK join.

## Snowflake Data Type Conversion

When the target data model specifies a data type that is not natively supported by Snowflake, convert it to the Snowflake equivalent when writing the STM's `Data Type` column. Apply this mapping everywhere a target-column data type is emitted (Field-Level Mapping Matrix and any later sections).

| Source Type (in target model) | Snowflake Type |
|---|---|
| `BIGINT` | `NUMBER(19,0)` |
| `VARBINARY`, `VARBINARY(n)` | `BINARY` |
| Hash **keys** (`HashPK`, `HashBK`, `HashFK`) | `NUMBER(19,0)` (from Snowflake's native `HASH(...)` — 64-bit non-cryptographic, picked for join performance and micro-partition pruning) |
| `Hashbytes` (change detection) | `BINARY(32)` (stores 32-byte SHA-256 digest from `CAST(SHA2_BINARY(..., 256) AS BINARY(32))` — SHA-256 collision strength is required because we compare Hashbytes to detect row changes) |

Notes:
- Hash **keys** (`HashPK`, `HashBK`, `HashFK`) use Snowflake's native `HASH(...)` function which returns a `NUMBER(19,0)` (signed 64-bit integer). Picked over `SHA2_BINARY` for keys because a 64-bit NUMBER is ~4× narrower than `BINARY(32)`, gives faster equality joins, and prunes micro-partitions better on keyed lookups. Collision bound is 2^63 ≈ 9.2 × 10^18 — safe below ~1B rows per table.
- `Hashbytes` uses `CAST(SHA2_BINARY(..., 256) AS BINARY(32))`. The explicit cast to `BINARY(32)` is required because Snowflake's `SHA2_BINARY` signature returns `BINARY(64)` by default (to cover SHA-512); the cast narrows the declared type to match the 32-byte SHA-256 digest we actually store. SHA-256's cryptographic collision strength is required here because we compare Hashbytes values to detect row changes.
- Do NOT use `VARCHAR(64)`/hex or `MD5` for either keys or Hashbytes. Do NOT use `SHA2_BINARY` for keys (too wide, slower joins). Do NOT use `HASH()` for Hashbytes (insufficient collision strength for change detection).
- Leave already-supported Snowflake types unchanged (`NUMBER(p,s)`, `VARCHAR(n)`, `DATE`, `TIMESTAMP_*`, `BOOLEAN`, etc.).
- Add new source→Snowflake mappings to this table as new source systems are onboarded; do not guess.

## Template Rules

Each generated STM must include:
1. Document Information
2. Business Context
3. Source System Inventory
4. Target Schema Definition
5. Classification Tags
6. Glossary Terms
7. Field-Level Mapping Matrix (includes a **Constraints** column for model- or catalogue-derived field rules)
8. Transformation & Business Rules
9. Data Quality & Validation Rules
10. Load Strategy
11. Version Control & Governance
12. Sign-Off

Use the user-provided STM structure exactly in spirit, but only populate values supported by the model markdown, analyzer JSON, and OpenMetadata API.
