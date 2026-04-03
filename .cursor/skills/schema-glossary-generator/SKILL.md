---
name: schema-glossary-generator
description: Generate a business glossary JSON and Excel workbook from a short industry, domain, or business-process description. Use when the user wants Cursor to author reusable glossary terms and classifications that are independent of any source system schema.
---

# Domain Glossary Generator

## Overview

Use this skill when the agent should read a short business description and author a canonical glossary itself. The skill exists to:
- tell the agent how to analyze a domain or business-process brief
- constrain the output to a fixed glossary JSON shape
- optionally export the finished glossary JSON to Excel

The agent should infer glossary content from:
- industry or business domain description
- business-process scope
- operating model and lifecycle language
- entities, documents, measures, statuses, and identifiers named or implied by the brief
- clearly implied relationships between those concepts

The agent should not depend on source-system tables, columns, or schema metadata. The goal is a reusable, business-facing glossary that can later be mapped to one or more source systems.

## Workflow

1. Read the input business brief.
2. Identify high-signal business entities, processes, documents, attributes, measures, statuses, identifiers, and important relationships.
3. Draft glossary entries in the fixed JSON shape described in `references/output-contract.md`.
4. Keep only supported or clearly inferred terms.
5. Mark weakly supported entries as inferred.
6. Write the glossary JSON beside the input.
7. Export that glossary JSON to Excel with:

```bash
python3 .cursor/skills/schema-glossary-generator/scripts/glossary_json_to_excel.py \
  <glossary_json> \
  <output_xlsx>
```

## Input

The preferred input is a short plain-language business brief. This can be:
- a `.txt` or `.md` description
- a `.json` object with fields such as `domain`, `description`, `core_processes`, `entities`, or `notes`

Minimal input can be as small as a few sentences. Listing core processes improves coverage, but it is not required.

## Output

The agent should produce:
- `<input_stem>_glossary.json`
- `<input_stem>_glossary.xlsx`

The JSON must follow the expected columns in `references/output-contract.md`.

## Glossary Rules

- Prefer business-facing terms over system-specific labels.
- The agent should generate the glossary content itself; scripts are helpers for formatting/export, not the primary author.
- Terms should be independent of any one source application or schema.
- Merge repeated concepts such as `email`, `phone`, and `status` into one glossary row when they represent the same business concept across the domain.
- Generate canonical terms for strong domain entities and workflows such as customers, products, suppliers, orders, invoices, inventory, fulfillment, procurement, and payments when supported by the brief.
- Use only the expected columns from the output contract. Do not add ad hoc fields.
- Do not invent unsupported policy or process rules. If a term is inferred from business context, mark it accordingly.

## Fallback Helper

`scripts/generate_glossary.py` remains available only as a deterministic draft/bootstrap helper when the user explicitly wants a machine-generated first pass from a domain brief. It is not the preferred workflow for this skill.
