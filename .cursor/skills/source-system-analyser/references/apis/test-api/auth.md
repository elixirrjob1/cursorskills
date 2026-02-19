# Test API Auth

## Base URL

- Default: `https://skillssimapifilip20260218.azurewebsites.net`
- Preferred env var: `API_BASE_URL`

## Authentication

- Required header: `Authorization: Bearer <token>`
- Token reference name: user-provided env var or Key Vault secret name (no defaults)

## Preflight

1. Resolve `API_BASE_URL` (or use default).
2. Ask for and confirm the exact bearer reference name (env var or Key Vault secret name).
3. Resolve bearer token from env/Key Vault using that exact reference.
4. Run a quick `GET /api/tables` check before bulk reads.

## Persist Confirmed Names

After a user confirms a reference name, save the name to:
- `references/apis/test-api/discovered-references.md`

Save names only. Never write secret values.
