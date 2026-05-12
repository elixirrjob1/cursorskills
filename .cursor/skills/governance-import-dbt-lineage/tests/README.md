# governance-import-dbt-lineage tests

This folder contains unit tests for:

- view-model detection
- view-flattening default behavior
- include-views behavior
- column-lineage mapping builder

## Run tests

From the repository root:

```bash
py -m unittest discover -s ".cursor/skills/governance-import-dbt-lineage/tests" -p "test_*.py"
```

Expected result:

- `Ran 4 tests`
- `OK`
