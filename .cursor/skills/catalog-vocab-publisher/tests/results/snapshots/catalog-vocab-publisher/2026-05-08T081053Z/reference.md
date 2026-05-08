# OpenMetadata API Reference — Classifications & Tags

## Base URL

```
https://<your-openmetadata-host>
```

All endpoints are prefixed with `/api`. Example:

```
https://sandbox.open-metadata.org/api/v1/classifications
```

---

## Authentication

The bundled `publish_vocab.py` authenticates with `POST /api/v1/users/login` (password is Base64-encoded in the JSON body per OpenMetadata’s login contract). It uses the returned `accessToken` as `Authorization: Bearer <token>` on all subsequent requests.

For manual `curl` or other clients, use the same bearer header:

```
Authorization: Bearer <token>
Content-Type: application/json
```

---

## Classifications

### GET by name

```
GET /api/v1/classifications/name/{classificationName}
```

Returns `200` with the classification object if it exists, `404` if not.

### PUT (create or update)

```
PUT /api/v1/classifications
```

**Payload:**

```json
{
  "name": "Architecture",
  "displayName": "Architecture",
  "description": "Governance classification: Architecture.",
  "mutuallyExclusive": true,
  "provider": "user"
}
```

| Field | Type | Notes |
|---|---|---|
| `name` | string | Short taxonomy label, no spaces. Must be unique. |
| `displayName` | string | Same as `name` — human-readable label. |
| `description` | string | Plain text description of the classification. |
| `mutuallyExclusive` | boolean | `true` = only one tag from this classification per asset. |
| `provider` | string | Always `"user"` for custom classifications. |

---

## Tags

### GET by FQN

```
GET /api/v1/tags/name/{fqn}
```

FQN format: `ClassificationName.TagName` — e.g., `Architecture.Raw`

Returns `200` with the tag object if it exists, `404` if not.

### PUT (create or update)

```
PUT /api/v1/tags
```

**Payload:**

```json
{
  "name": "Raw",
  "displayName": "Raw",
  "description": "Unprocessed data as received from upstream.\n\nAd impression logs ingested directly from DSP bid-stream APIs.",
  "classification": "Architecture",
  "provider": "user"
}
```

| Field | Type | Notes |
|---|---|---|
| `name` | string | Tag label. Unique within the classification. |
| `displayName` | string | Same as `name`. |
| `description` | string | Plain text + domain context joined with `\n\n`. |
| `classification` | string | Name of the parent classification. |
| `provider` | string | Always `"user"`. |

---

## FQN format

| Entity | FQN |
|---|---|
| Classification | `Architecture` |
| Tag | `Architecture.Raw` |
| Nested tag | `Architecture.Raw.Unvalidated` *(not used by this skill)* |

---

## Response fields used for deduplication

When GETting an existing classification or tag, the script compares returned fields against the parsed `.md` data: for classifications, `mutuallyExclusive`; for tags, `description`. If unchanged, the PUT is skipped.
