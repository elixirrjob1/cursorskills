# Eval Suite: governance-vocab-generator

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Generate a data governance vocabulary for the retail banking domain." | Evaluates canonical dimensions for banking (Privacy, Criticality, ComplianceLegal, Retention, QualityTrust); writes `.md` to `governance-vocabularies/retail-banking-governance-vocab.md`; returns only the file path. |
| 2 | "Create governance taxonomy for our data platform — it uses Bronze/Silver/Gold medallion architecture." | Includes `Architecture` classification with Bronze/Silver/Gold levels (Medallion labels permitted per skill rule); returns only the file path. |
| 3 | "We need a classification framework for our digital health data catalog." | Triggers; includes `Privacy` (health data contains personal/sensitive data) and at least one further canonical dimension; returns only the file path — no prose explanation. |
| 4 | "Produce a governance vocabulary for a legal document management system." | Omits `Architecture` (no staged data processing pipeline); includes `ComplianceLegal`, `Retention`, `Privacy`; returns file path only. |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Apply the Privacy and ComplianceLegal governance tags to all the tables in our OpenMetadata catalog." | Tag assignment to existing catalog assets — handled by `catalog-glossary-tagger` or `catalog-sync`, not vocab generation. Response does NOT produce a vocabulary `.md` file. |
| 2 | "Build a dbt SQL model that tracks data classification levels in our Snowflake warehouse." | SQL transformation request — handled by `dbt-model-from-stm` or general SQL work. Response does NOT generate a governance vocabulary markdown. |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Generate a governance vocabulary for IoT sensor data — also add a custom SensorType classification." | Evaluates `SensorType` against the 3-condition test (all three conditions required for non-canonical). `SensorType` describes physical device categories, not a governance practice structurally distinct to IoT — fails condition 2; omitted. Canonical `Architecture` (raw sensor → processed → aggregated) and `QualityTrust` (sensor reliability) are included. |
| 2 | "Create a governance classification framework for HR data — include all 7 canonical dimensions." | Does NOT blindly include all 7 just because the user requested it. Evaluates each dimension: `Architecture` likely omitted (HR systems typically lack staged processing pipelines); `Privacy` and `Criticality` included; final output is the assessed subset. Returns file path only. |
