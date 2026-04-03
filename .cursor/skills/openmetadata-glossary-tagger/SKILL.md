---
name: openmetadata-glossary-tagger
description: Read glossary terms and imported table metadata from the OpenMetadata MCP server, inspect full table payloads including columns and descriptions, decide glossary-term matches, and assign glossary labels back to tables or columns in OpenMetadata. Use when the user wants Cursor to map business glossary terms onto cataloged database assets through OpenMetadata.
---

# OpenMetadata Glossary Tagger

Use the OpenMetadata MCP server as the execution surface for glossary governance.

## Prerequisites

Before running the workflow:
- ensure the `openmetadata` MCP server is registered in Cursor
- ensure `OPENMETADATA_BASE_URL`, `OPENMETADATA_EMAIL`, and `OPENMETADATA_PASSWORD` are available
- ensure metadata has already been ingested into OpenMetadata for the target service and schema

If the MCP server is not configured yet, use:

```bash
./scripts/install_openmetadata_mcp_deps.sh
./scripts/setup_openmetadata_mcp.sh
```

## Workflow

1. Validate connectivity with `test_connection`.
2. Read the governance source first:
   - `list_glossaries`
   - `list_glossary_terms`
   - `get_glossary_term` when description or synonyms are needed to disambiguate a term
3. Read the metadata to tag:
   - `list_tables` for the target schema
   - treat each returned table as the primary source for table name, table description, columns, and column descriptions
   - `get_table` only when more detail is needed or to re-check a single table after updates
   - `get_column` only when a column needs closer inspection
4. Match terms conservatively:
   - use glossary term descriptions and synonyms as the semantic definition
   - use table and column descriptions as the strongest evidence on the metadata side
   - use names as supporting evidence, not sole proof
5. Apply glossary labels:
   - `assign_glossary_term_to_table` for table-level business concepts
   - `assign_glossary_term_to_column` for column-level business concepts
6. Re-read updated entities to confirm the labels landed.
7. Report what was applied, what was skipped, and what needs review.

## Matching Rules

- Prefer precise matches over broad lexical similarity.
- Prefer column-level tags when the concept is specific to one field.
- Use table-level tags when the whole table clearly represents the business concept.
- Do not force a match when descriptions are missing and names are ambiguous.
- Do not remove existing labels unless the user explicitly asks for cleanup.
- If the same glossary label is already present, leave it unchanged.

## Bulk Tagging Pattern

For large schemas:
1. Read glossary terms in scope.
2. Read all target tables with `list_tables`.
3. Build a candidate mapping table-by-table and column-by-column.
4. Auto-apply only high-confidence matches.
5. Surface ambiguous matches for review instead of guessing.

## Output Contract

Return:
- applied table glossary tags
- applied column glossary tags
- skipped or ambiguous entities
- brief evidence for each applied tag

## Guardrails

- Do not treat glossary terms and classification tags as interchangeable.
- Do not invent glossary terms in OpenMetadata unless the user explicitly asks to create them.
- Do not rely on names alone when the same term could refer to multiple business concepts.
