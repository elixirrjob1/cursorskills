---
name: governance-vocab-generator
description: >-
  Generate domain-informed data governance classification vocabularies as Markdown files for data catalogs.
  Use when asked to produce classification frameworks, governance taxonomies, data catalog
  vocabularies, classification schemas, or governance dimensions for a given business domain.
  Triggers: data catalog, governance vocabulary, classification framework, data governance,
  taxonomy for data assets, governance schema, classification levels.
---

# Governance Vocabulary Generator

Produces a domain-specific classification framework as a `.md` file for a data catalog, aligned with software and data platform governance practices. Vocabulary only — no asset assignments.

## Output contract

Write a `.md` file to `governance-vocabularies/<domain-slug>-governance-vocab.md`. Return only the file path when done — no prose, no JSON, no additional explanation.

Use this structure exactly:

```markdown
# [Domain] — Data Governance Vocabulary

---

## [Classification Name]
*Mutually exclusive* OR *Multi-select*

### [Level Name]
[Description: what this state means in governance practice]

> [Domain context: why this level is meaningful in the given domain]

### [Next Level Name]
...

---

## [Next Classification Name]
...
```

Rules:
- Use `*Mutually exclusive*` when `mutually_exclusive: true`, `*Multi-select*` otherwise.
- Separate each classification with `---`.
- Description is plain text on its own line. Domain context is a `>` blockquote directly below it.
- No tables, no JSON, no extra headings beyond those shown above.

---

## Canonical dimensions

When a classification aligns with one of these concepts, use the exact name — no synonyms:

| Exact name | Concept |
|---|---|
| `Architecture` | Data progression through pipeline stages |
| `Privacy` | Personal data sensitivity and regulatory exposure |
| `Criticality` | Business impact if data is unavailable or incorrect |
| `Lifecycle` | State of the data asset in its operational lifespan |
| `Retention` | How long data must or should be kept |
| `QualityTrust` | Confidence in data accuracy, completeness, and reliability |
| `ComplianceLegal` | Regulatory, legal, or contractual obligations |

---

## Classification design rules

Start from the canonical dimensions. For each one, assess whether it applies to the domain before considering anything else.

Only introduce a non-canonical classification when **all three** of the following are true:

1. The concept is not covered by any canonical dimension, even partially.
2. The classification reflects a governance practice that is **structurally distinct to the domain** — not merely a domain-specific way of labelling a universal concept.
3. The absence of the classification would leave a material governance gap that cannot be addressed by tailoring canonical levels.

If a concept could be expressed as levels within a canonical classification, do that instead of adding a new classification.

Additional rules:
- Names must be **short taxonomy labels** (e.g., `IngestionPattern`, `ValuationBasis`).
- Generic or domain-agnostic names are not allowed.
- Omit any classification whose levels would be weak, generic, or transferable to another domain unchanged.
- When in doubt, omit. Fewer well-defined classifications are preferable to more weak ones.

---

## Level design rules

Levels must:
- Represent a distinct, non-overlapping state
- Reflect a real difference in how data is handled, interpreted, accessed, or used
- Correspond to a distinction observed in governance or data handling practice

Do **not**:
- Assume a fixed number of levels
- Balance level counts across classifications
- Use generic patterns such as Low / Medium / High or Public / Internal / Confidential unless those exact terms are the domain-standard vocabulary
- Introduce a level without a clear governance purpose

---

## Architecture guidance

Include `Architecture` only when the domain involves **staged data processing** (e.g., ingestion → refinement → consumption).

Levels describe distinct processing stages — not business domains, org units, or application groupings.

Medallion labels (Bronze / Silver / Gold) are acceptable only when the domain clearly uses that pattern.

Each level must reflect a **meaningful change** in processing, structure, or usability — not just a name.

---

## Workflow

1. Identify the domain from the user's request.
2. Evaluate each canonical dimension in turn. Include it if it applies; omit it if the domain has no meaningful distinction to express under that concept.
3. After exhausting canonical dimensions, apply the three-condition test to identify any non-canonical classifications genuinely required by domain-wide governance practice. Default to omitting.
4. For each classification, determine the correct number of levels based on how it is structured in practice for this domain.
5. Write domain context entries that are **specific to the domain** — not generic restatements of the description.
6. Write the vocabulary to `governance-vocabularies/<domain-slug>-governance-vocab.md` using the output format above. Return the file path only.

---

## Example skeleton (do not copy levels — generate domain-specific ones)

```markdown
# Digital Advertising — Data Governance Vocabulary

---

## Architecture
*Mutually exclusive*

### Raw
Unprocessed event data as received from upstream sources.

> Ad impression and click logs ingested directly from DSP bid-stream APIs before deduplication or join enrichment.

---

## Privacy
*Mutually exclusive*

### Direct Identifier
Data that directly identifies a natural person without additional linking.

> Hashed email addresses (HEMs) and device IDs used for identity resolution in audience targeting.
```
