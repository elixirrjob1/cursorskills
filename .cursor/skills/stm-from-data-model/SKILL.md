---
name: stm-from-data-model
description: Generate source-to-target mapping markdown documents from a target data-model markdown file. Use when the user has a dimensional/star-schema model in markdown and wants one STM document per target table, written to `stm/output`, filling only what can be derived from the input and leaving unknown fields blank.
---

# STM From Data Model

## When to use

Use this skill when:
- the input is a markdown data model, not a source schema dump
- the user wants STM/source-to-target mapping documents per target table
- the output should follow a fixed STM template
- unknown source-side values must remain blank

## Inputs

- Default input folder: `/home/fillip/stm/input`
- Default output folder: `/home/fillip/stm/output`
- The preferred input is one markdown file describing the target warehouse model

If the caller provides explicit paths, use them. Otherwise:
- read the single `.md` file in `stm/input`
- write all outputs to `stm/output`

## Run

```bash
python3 .cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py \
  --input /home/fillip/stm/input/<model>.md \
  --output-dir /home/fillip/stm/output
```

If there is exactly one markdown file in `/home/fillip/stm/input`, `--input` may be omitted:

```bash
python3 .cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py
```

## Output

The script generates:
- one STM markdown file per target table
- `README.md` in the output directory listing generated files

File naming:
- `01-<TableName>-stm.md`
- `02-<TableName>-stm.md`
- etc.

## Population Rules

- Fill only fields derivable from the input model.
- Leave unknown values literally blank.
- Do not invent source systems, source tables, source columns, owners, orchestration, DQ thresholds, or OpenMetadata tags.
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
5. Field-Level Mapping Matrix
6. Transformation & Business Rules
7. Data Quality & Validation Rules
8. Load Strategy
9. Version Control & Governance
10. Sign-Off

Use the user-provided STM structure exactly in spirit, but only populate values supported by the model markdown.
