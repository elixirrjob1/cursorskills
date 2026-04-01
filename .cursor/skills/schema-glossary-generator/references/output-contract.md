# Output Contract

The agent must write glossary JSON with this top-level shape:

```json
{
  "metadata": {
    "generated_at": "",
    "source_schema_json": "",
    "generation_mode": "agent_authored",
    "inference_mode": "schema_guided"
  },
  "entries": []
}
```

Each item in `entries` must use exactly these fields:

```json
{
  "term": "",
  "term_type": "",
  "definition": "",
  "business_usage": "",
  "synonyms": [],
  "source_tables": [],
  "source_columns": [],
  "confidence": 0.0,
  "confidence_tier": "",
  "inference_basis": "",
  "source_refs": [],
  "notes": "",
  "status": ""
}
```

Expected values:

- `term_type`: `business_entity`, `business_process`, `business_measure`, `business_attribute`, `status`, `identifier`, `relationship`, `inferred_concept`
- `confidence_tier`: `high`, `medium`, `low`
- `status`: `confirmed_from_schema`, `inferred_from_schema`

Authoring rules:

- `definition` should be business-facing, concise, and grounded in schema evidence.
- `business_usage` should explain where the term is used in the business process or reporting context.
- `synonyms` should include aliases or alternate labels when clearly supported.
- `source_tables`, `source_columns`, and `source_refs` should point back to the schema evidence.
- `inference_basis` should summarize why the agent believes the term is valid.
- Use only these fields. Do not add custom fields.
