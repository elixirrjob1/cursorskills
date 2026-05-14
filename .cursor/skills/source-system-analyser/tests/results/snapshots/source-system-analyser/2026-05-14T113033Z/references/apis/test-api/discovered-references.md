# Discovered References

Store confirmed reference names for this API module here.

## Rules

- Store names only (env var names, Key Vault names, secret names).
- Never store secret values, tokens, keys, or connection strings.
- If a reference is unconfirmed, mark it as `pending`.

## Current

- `api_base_url_env`: `API_BASE_URL` (confirmed)
- `key_vault_name`: `pending`
- `bearer_env_name`: `pending`
- `bearer_secret_name`: `pending`

## Update Template

- `api_base_url_env`: `<NAME>` (`confirmed`|`pending`)
- `key_vault_name`: `<NAME>` (`confirmed`|`pending`)
- `bearer_env_name`: `<NAME>` (`confirmed`|`pending`)
- `bearer_secret_name`: `<NAME>` (`confirmed`|`pending`)
- `notes`: `<optional>`
