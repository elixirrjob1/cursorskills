# Output Contract

The agent must write glossary JSON with this top-level shape:

```json
{
  "metadata": {
    "generated_at": "",
    "source_description_path": "",
    "generation_mode": "agent_authored",
    "inference_mode": "domain_guided"
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
  "notes": "",
  "status": ""
}
```

Expected values:

- `term_type`: `business_entity`, `business_process`, `business_measure`, `business_attribute`, `status`, `identifier`, `relationship`, `inferred_concept`
- `status`: `draft`, `approved`, `rejected`

Authoring rules:

- `definition` should be business-facing, concise, and grounded in the business brief.
- `business_usage` should explain where the term is used in the business process or reporting context.
- `synonyms` should include aliases or alternate labels when clearly supported.
- Use only these fields. Do not add custom fields.
