# Troubleshooting

## Connection Failures

- Verify host/port and network reachability.
- Confirm driver is installed for selected dialect.
- Check credentials and URL encoding for special characters.

## Empty or Partial Results

- Validate schema filter (`DATABASE_SCHEMA` / `SCHEMA`).
- Ensure user has metadata/table read permissions.
- Retry without aggressive filters and inspect row-count queries.

## Quality Check Gaps

- Some checks depend on row sampling and may degrade on restricted permissions.
- API/flat generic routes may not infer all relational constraints.

## Timezone Ambiguity

- If server timezone is unavailable, annotate as unknown and continue.
- Prefer explicit timezone metadata from source columns when present.
