# Retail Banking — Data Governance Vocabulary

---

## Architecture
*Mutually exclusive*

### Source Extract
Raw data as received from core banking systems, payment processors, or third-party feeds — no transformations applied.

> Unmodified transaction journals, card authorisation logs, and account master exports from upstream operational systems before reconciliation.

### Curated
Data that has been reconciled, de-duplicated, joined across source systems, and standardised to the bank's canonical data model.

> Customer 360 records assembled from CRM, KYC, and account systems; intraday settlement positions reconciled against RTGS feeds.

### Reporting
Aggregated, KPI-ready data structured for regulatory submission, management reporting, or analytical consumption.

> Capital adequacy ratios, loan-loss provisioning summaries, and product profitability metrics prepared for regulatory returns and board packs.

---

## Privacy
*Mutually exclusive*

### Personal Financial Data
Data that identifies a natural person and reveals their financial position, behaviour, or history.

> Account balances, transaction histories, credit scores, loan terms, and beneficial ownership records held under GDPR Article 9 financial-data obligations.

### Pseudonymised Financial Data
Financial data where direct identifiers have been replaced with tokens, but re-identification is possible with the linkage key.

> Tokenised account numbers used in analytics pipelines where the vault key is held in a separate controlled environment.

### Aggregate or Statistical
Data derived from many individuals where individual re-identification is not feasible.

> Branch-level average deposit balances, product uptake rates by postal district, and cohort-level credit risk distributions.

### Non-Personal Operational
Data with no connection to individual customers — market data, internal reference tables, system metadata.

> FX spot rates, benchmark interest rates, product tariff schedules, and GL account hierarchies.

---

## Criticality
*Mutually exclusive*

### Core Banking
Data whose unavailability or inaccuracy would directly impair customer account operations or settlement.

> Real-time account balances, payment instruction queues, and authorisation limits — corruption or loss would prevent transactions from completing.

### Regulatory Mandatory
Data required to fulfil a statutory reporting obligation with a defined submission deadline.

> FINREP, AnaCredit, and transaction reporting datasets submitted to prudential and conduct regulators; delay or error carries direct supervisory consequences.

### Risk and Audit
Data used to compute risk exposures, support internal audit trails, or evidence compliance controls.

> Trade reconciliation records, AML suspicious activity logs, and capital model inputs — loss would impair the bank's control attestation.

### Management Analytical
Data used for internal decision-making, planning, or performance management with no direct regulatory dependency.

> Customer profitability dashboards, product P&L attributions, and sales pipeline forecasts.

---

## Lifecycle
*Mutually exclusive*

### Active
Data currently used in operational processes or live regulatory reporting cycles.

> Open accounts, current-period transaction records, and in-scope regulatory exposures for the live reporting quarter.

### Historical
Data no longer updated but retained for trend analysis, audit reference, or regulatory look-back periods.

> Closed account records within the statutory retention window; prior-year regulatory return inputs.

### Archived
Data moved to low-cost storage following expiry of active retention periods, accessible only by formal request.

> Customer records archived after account closure and satisfaction of the full statutory retention period, awaiting final purge approval.

### Purged
Data that has been irreversibly deleted in accordance with an approved retention schedule.

> Personal data erased following a verified right-to-erasure request or expiry of the maximum retention period.

---

## Retention
*Mutually exclusive*

### Regulatory Mandated
Retention period and format prescribed by statute or supervisory regulation.

> Transaction records subject to MiFID II seven-year retention; AML customer due diligence records subject to five-year post-relationship-end retention under AMLD.

### Contractual
Retention period agreed with a counterparty or customer under contract terms.

> Mortgage account data retained for the duration of the loan plus the contractually specified post-closure period.

### Operational
Retained at the bank's discretion for business purposes in the absence of a regulatory or contractual obligation.

> Internal management accounts and non-regulatory analytical datasets retained per the bank's data management policy.

### Transient
Data with no required retention period — to be deleted after its immediate processing purpose is fulfilled.

> Intraday payment routing scratch records and real-time fraud score inputs that must not persist beyond the transaction window.

---

## QualityTrust
*Mutually exclusive*

### Authoritative
Data produced by the system of record for this information type — the single trusted source for downstream consumers.

> The core banking platform's account balance as the authoritative source for all downstream risk, regulatory, and analytics uses.

### Reconciled
Data validated against an authoritative source; discrepancies have been investigated and resolved.

> Payment settlement positions after end-of-day RTGS reconciliation confirming agreement with correspondent bank statements.

### Derived
Computed or inferred from authoritative sources; accuracy is contingent on the quality of the upstream inputs and the transformation logic.

> Expected credit loss calculations, internal credit ratings, and behavioural segmentation scores.

### Indicative
Data sourced from external or third-party feeds where the bank cannot fully verify provenance or completeness.

> Third-party credit bureau scores, market data vendor feeds, and property valuation indices used in credit decisioning.

---

## ComplianceLegal
*Multi-select*

### GDPR Regulated
Data subject to EU General Data Protection Regulation obligations — lawful basis documented, data subject rights applicable.

> Customer personal data processed under contractual necessity or legitimate interest; subject access and erasure rights must be honoured.

### AML / KYC Obligated
Data generated or used as part of anti-money laundering and know-your-customer obligations under AMLD or equivalent national law.

> Customer due diligence records, PEP and sanctions screening outputs, and suspicious activity reports — subject to tipping-off prohibitions.

### PSD2 / Open Banking Scoped
Data accessed or shared under payment services regulation — subject to strong customer authentication and third-party access rules.

> Payment account data shared with regulated TPPs via Open Banking APIs; consent records and access logs must be retained.

### Contractually Restricted
Data whose use, sharing, or retention is constrained by a specific contractual obligation with a third party.

> Data received under NDA from a financial data vendor; correspondent bank data shared under bilateral information-sharing agreements.

### Unrestricted
No regulatory, statutory, or contractual constraint on use or sharing beyond the bank's internal data policies.

> Publicly available market reference data and internally produced management commentary with no personal data content.
