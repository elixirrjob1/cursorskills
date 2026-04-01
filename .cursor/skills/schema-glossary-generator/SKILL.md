---
name: schema-glossary-generator
description: Generate a business glossary JSON and Excel workbook from analyzer schema JSON. Use when the user has a schema analyzer `.json` file and wants the Cursor agent to analyze it and write glossary terms, descriptions, usage notes, confidence, and source references into a fixed output shape.
---

# Schema Glossary Generator

## Overview

Use this skill when the agent should read analyzer schema JSON and author a glossary itself. The non-deterministic part is the agent analysis, not a fixed rules engine. The skill exists to:
- tell the agent how to analyze the schema
- constrain the output to a fixed glossary JSON shape
- optionally export the finished glossary JSON to Excel

The agent should infer glossary content from:
- `concept_registry`
- `tables[].table_description`
- `tables[].columns[].column_description`
- `semantic_class`
- `concept_id`
- join relationships and foreign keys
- table and column naming context

The agent should not treat the task as pure script execution. It should read the schema JSON, reason over the model, and write glossary entries in a business-facing style while staying grounded in the schema evidence.

## Workflow

1. Read the input analyzer JSON.
2. Identify high-signal business entities, processes, attributes, measures, statuses, identifiers, and important relationships.
3. Draft glossary entries in the fixed JSON shape described in `references/output-contract.md`.
4. Keep only supported or clearly inferred terms.
5. Mark weakly supported entries as inferred and lower confidence.
6. Write the glossary JSON beside the input.
7. Export that glossary JSON to Excel with:

```bash
python3 .cursor/skills/schema-glossary-generator/scripts/glossary_json_to_excel.py \
  <glossary_json> \
  <output_xlsx>
```

## Output

The agent should produce:
- `<input_stem>_glossary.json`
- `<input_stem>_glossary.xlsx`

The JSON must follow the expected columns in `references/output-contract.md`.

## Glossary Rules

- Prefer business-facing terms over SQL-only labels.
- The agent should generate the glossary content itself; scripts are helpers for formatting/export, not the primary author.
- Merge repeated concepts such as `email`, `phone`, and `status` into one glossary row when they represent the same business concept across tables.
- Generate table-derived terms for strong domain entities and workflows such as customers, products, stores, suppliers, inventory, purchase orders, and sales orders.
- Use only the expected columns from the output contract. Do not add ad hoc fields.
- Include confidence and source references for every row.
- Do not invent unsupported business policy or process rules. If a term is inferred from schema context, mark it accordingly.

## Fallback Helper

`scripts/generate_glossary.py` remains available only as a deterministic draft/bootstrap helper when the user explicitly wants a machine-generated first pass. It is not the preferred workflow for this skill.
