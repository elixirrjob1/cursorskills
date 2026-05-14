# API Auth Patterns

Support these auth mechanisms in priority order:

1. Bearer token from env or key vault
2. API key header
3. Basic auth if explicitly required

Never write tokens into tracked files.

If a reference name is confirmed, save that name (not value) to:
- `references/apis/generic/discovered-references.md`
