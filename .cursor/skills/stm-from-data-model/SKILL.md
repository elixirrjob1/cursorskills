---
name: stm-from-data-model
description: Generate source-to-target mapping markdown documents from a target data-model markdown file plus analyzer schema JSON. Use when the user has a dimensional/star-schema model in markdown, an analyzer JSON with glossary and classification assignments, and wants one STM document per target table written to `stm/output`.
---

# STM From Data Model

## When to use

Use this skill when:
- the inputs are a markdown data model plus analyzer schema JSON
- the user wants STM/source-to-target mapping documents per target table
- the output should follow a fixed STM template
- analyzer glossary terms and classification tags should be copied into the STM
- unknown source-side values must remain blank, except for the hardcoded Snowflake source/target conventions below

## Inputs

- Default input folder: `stm/input` (relative to project root)
- Default output folder: `stm/output` (relative to project root)
- Required inputs:
  - one markdown file describing the target warehouse model
  - one analyzer schema JSON file with table/column `glossary_terms` and `classification_tags`
- Environment requirements for glossary definitions:
  - `OPENMETADATA_BASE_URL`
  - `OPENMETADATA_EMAIL` and `OPENMETADATA_PASSWORD`, or `OPENMETADATA_JWT_TOKEN`

If the caller provides explicit paths, use them. Otherwise:
- read the single `.md` file in `stm/input`
- read the single `.json` file in `stm/input`
- write all outputs to `stm/output`

## Run

```bash
python3 .cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py \
  --input stm/input/<model>.md \
  --analyzer-json LATEST_SCHEMA/schema_azure_mssql_dbo.json \
  --output-dir stm/output
```

If there is exactly one markdown file and one analyzer JSON file in `stm/input`, both path flags may be omitted:

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

## Population Rules

Hardcode these warehouse conventions in every generated STM:
- `Source System Inventory.Source System` = `Snowflake`
- `Source System Inventory.Database / Schema` = `DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO`
- `Source System Inventory.Table / File` = `See field-level mapping`
- `Source System Inventory.Notes` = `Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system.`
- `Target Schema Definition.Target Database` = `DRIP_DATA_INTELLIGENCE`
- `Target Schema Definition.Schema` = `GOLD`

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

## Snowflake Data Type Conversion

When the target data model specifies a data type that is not natively supported by Snowflake, convert it to the Snowflake equivalent when writing the STM's `Data Type` column. Apply this mapping everywhere a target-column data type is emitted (Field-Level Mapping Matrix and any later sections).

| Source Type (in target model) | Snowflake Type |
|---|---|
| `BIGINT` | `NUMBER(38,0)` |
| `VARBINARY`, `VARBINARY(n)` | `BINARY` |
| Hash columns (`HashPK`, `HashBK`, `HashFK`, `Hashbytes`) | `BINARY(32)` (stores raw 32-byte digest from `SHA2_BINARY(..., 256)`) |

Notes:
- Hash columns use `SHA2_BINARY(..., 256)` which returns a native 32-byte `BINARY` value. `SHA2_BINARY` is a dedicated Snowflake function (not a cast), so it does **not** depend on the session `BINARY_INPUT_FORMAT` — it is portable and safe.
- Do NOT use `VARCHAR(64)`/hex or `MD5`. The team standard is `BINARY(32)` via `SHA2_BINARY(..., 256)`.
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
7. Field-Level Mapping Matrix
8. Transformation & Business Rules
9. Data Quality & Validation Rules
10. Load Strategy
11. Version Control & Governance
12. Sign-Off

Use the user-provided STM structure exactly in spirit, but only populate values supported by the model markdown, analyzer JSON, and OpenMetadata API.
