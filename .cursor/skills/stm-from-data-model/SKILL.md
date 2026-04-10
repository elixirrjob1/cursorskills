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
- unknown source-side values must remain blank

## Inputs

- Default input folder: `stm/input` (relative to project root)
- Default output folder: `stm/output` (relative to project root)
- Required inputs:
  - one markdown file describing the target warehouse model
  - one analyzer schema JSON file with table/column `glossary_terms` and `classification_tags`

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

- Fill only fields derivable from the model markdown and analyzer JSON.
- Leave unknown values literally blank.
- Do not invent source systems, source tables, source columns, owners, orchestration, DQ thresholds, glossary definitions, or classification tags.
- Do not query OpenMetadata during STM generation.
- Use analyzer JSON `classification_tags` to populate classification sections.
- Render only classification names and assigned tag FQNs in the classification section; do not expand classification definitions.
- Use analyzer JSON `glossary_terms` directly for glossary sections.
- If the analyzer JSON does not contain glossary definitions, leave the definition cell blank rather than inventing one.
- Fill transformation logic conservatively:
  - explicit formulas from the model
  - explicit SCD behavior
  - explicit grain statements
  - explicit measure notes

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

Use the user-provided STM structure exactly in spirit, but only populate values supported by the model markdown and analyzer JSON.
