# Data Platform — Data Governance Vocabulary

---

## Architecture
*Mutually exclusive*

### Bronze
Raw data as ingested from source systems — no transformations, no quality enforcement, full fidelity to the source payload.

> Source files, CDC events, and API responses landed directly into the raw zone; schema and content are preserved exactly as received, including errors and duplicates.

### Silver
Cleaned, validated, and conformed data — duplicates removed, types cast, referential integrity enforced, and joined to canonical reference data.

> Deduplicated event streams merged with entity master data; records that fail validation rules are quarantined rather than silently dropped.

### Gold
Aggregated, business-ready data modelled for analytical consumption — metrics, dimensions, and domain-specific aggregations aligned to agreed business definitions.

> Fact and dimension tables, pre-aggregated KPI datasets, and domain data products consumed by BI tools, data science workloads, and operational reporting.

---

## Privacy
*Mutually exclusive*

### Directly Identified
Data that identifies a natural person without any additional transformation or linkage.

> Raw CRM exports containing name, email, and account number ingested into Bronze; must not be replicated beyond the controlled raw zone without access review.

### Pseudonymised
Personal data where direct identifiers have been replaced with tokens; re-identification requires access to a separately controlled linkage table.

> Silver-layer records where customer IDs are replaced with platform-generated surrogate keys; the mapping table is access-controlled separately.

### Anonymised
Data from which personal identifiers have been irreversibly removed such that re-identification is not feasible.

> Aggregated cohort-level metrics and statistically perturbed datasets cleared for open analytical consumption in Gold.

### Non-Personal
No personal data content — reference data, system metadata, technical configuration, or fully aggregated statistics.

> Product catalogs, geo-reference tables, pipeline execution logs, and aggregate performance metrics with no individual-level data.

---

## Criticality
*Mutually exclusive*

### Mission Critical
Pipeline failure or data corruption would directly impair a customer-facing product, a regulatory obligation, or a time-sensitive operational process.

> Real-time fraud scoring feeds and end-of-day settlement inputs — SLA breach triggers immediate incident response.

### Business Critical
Unavailability would materially affect internal decision-making or planned business operations, but does not have an immediate customer or regulatory impact.

> Daily sales performance aggregations and financial close datasets — delay of hours is operationally significant.

### Operational
Used in regular internal workflows; degraded quality or availability causes inefficiency but not material business harm.

> Data quality monitoring dashboards, internal audit log datasets, and team-level KPI feeds.

### Informational
Exploratory, experimental, or reference data with no dependency from live business processes.

> Research sandbox datasets, proof-of-concept model outputs, and historical backfill data used for exploratory analysis.

---

## Lifecycle
*Mutually exclusive*

### Active
Currently in production use — actively written to, read from, or depended upon by downstream consumers.

> Live Bronze ingestion tables, current-period Silver conformed entities, and Gold data products with active BI consumers.

### Deprecated
Still accessible but superseded by a replacement; consumers have been notified and are expected to migrate within a defined window.

> Legacy dimensional model tables replaced by a new data product; maintained in read-only state during the migration period.

### Retired
No longer maintained or guaranteed to be current; preserved for historical reference only.

> Old Bronze landing zones from replaced source systems; no new data is written but records are retained per policy.

### Purged
Data has been deleted in line with the approved retention schedule or a data subject rights request.

> Personal data removed from Bronze and Silver layers following GDPR erasure requests; deletion confirmed across all replicas.

---

## Retention
*Mutually exclusive*

### Regulatory Mandated
Retention period and deletion criteria prescribed by law or regulation.

> Personally identifiable records subject to statutory minimum and maximum retention periods under applicable data protection law.

### Operational
Retained at the platform's discretion based on business utility and storage cost, in the absence of a legal obligation.

> Intermediate Silver tables and model training datasets retained while they support active analytical use cases.

### Hot Window
Short-lived data retained only for the duration of an active processing window or near-real-time query pattern.

> Streaming micro-batch staging tables and real-time feature store entries that must not persist beyond their processing SLA.

---

## QualityTrust
*Mutually exclusive*

### Certified
Data that has passed defined quality gates — completeness, accuracy, and timeliness checks enforced and documented.

> Gold-layer data products that have passed automated dbt test suites and been signed off by a data owner; safe for regulated and executive-level consumption.

### Validated
Quality checks have been applied and passed, but certification has not been formally issued — suitable for most analytical use.

> Silver-layer tables that pass schema and null-rate tests but have not completed the full data product certification workflow.

### Unchecked
No quality tests have been applied — raw or experimental data whose accuracy and completeness are unknown.

> Bronze ingestion tables immediately after landing; quality characteristics are those of the upstream source system.

### Experimental
Data produced for exploratory or prototype purposes; quality is not guaranteed and the dataset may be modified without notice.

> Feature engineering outputs and model training datasets in sandbox environments; not for use in operational or regulated workflows.

---

## ComplianceLegal
*Multi-select*

### GDPR Regulated
Personal data processed under GDPR — lawful basis documented, data subject rights applicable, cross-border transfer restrictions enforced.

> Customer records ingested into Bronze; personal data must not leave approved regions without a valid transfer mechanism.

### Contractually Restricted
Use, sharing, or retention constrained by a third-party data supply agreement or licensing terms.

> Licensed third-party datasets whose terms prohibit redistribution, sub-licensing, or use beyond the agreed purpose.

### Confidential Internal
No regulatory restriction, but classified as sensitive under internal data policy — access limited to authorised teams.

> Strategic planning datasets, compensation data, and pre-announcement financial forecasts.

### Unrestricted
No regulatory, contractual, or internal sensitivity constraint beyond standard platform access controls.

> Publicly sourced reference data, open datasets, and fully anonymised aggregate statistics.
