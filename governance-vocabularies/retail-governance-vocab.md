# Retail — Data Governance Vocabulary

---

## Architecture
*Mutually exclusive*

### Raw
Unprocessed data as received from source systems, with no transformation or validation applied.

> Transaction logs from POS systems, e-commerce order feeds, and supplier EDI messages prior to any cleansing or deduplication.

### Cleansed
Data that has been validated, deduplicated, and standardised but not yet integrated across sources.

> Product records normalised to a common unit of measure and customer records deduplicated within a single channel before cross-channel matching.

### Conformed
Data integrated across channels and aligned to enterprise master data — product master, customer master, and store hierarchy.

> Orders reconciled across in-store, online, and marketplace channels with a single customer key and a unified product catalogue reference.

### Serving
Data structured and optimised for consumption by BI tools, merchandising analytics, and demand forecasting models.

> Aggregated sales and margin fact tables, inventory availability snapshots, and customer lifetime value outputs exposed to reporting layers.

---

## Privacy
*Mutually exclusive*

### PaymentCardData
Data that falls within PCI-DSS scope — card numbers, authentication values, or cardholder verification data.

> Primary account numbers (PANs), CVV codes, and full magnetic stripe data captured at POS terminals or payment gateways.

### IdentifiedCustomer
Personal data that directly identifies a retail customer — name, contact details, loyalty account, or complete purchase history.

> Customer name, delivery address, email address, and loyalty card number linked to a full transaction history.

### PseudonymousCustomer
Data associated with a customer identifier that does not directly reveal identity but is re-identifiable through linkage.

> Cookie IDs, device fingerprints, and anonymised browsing sessions that can be re-linked to an identified customer via a loyalty or account join.

### NonPersonal
Operational, product, or supplier data that contains no personal data component.

> Inventory counts, planogram data, supplier lead times, and store-level sales aggregates with no customer linkage.

---

## Criticality
*Mutually exclusive*

### TradingCritical
Data whose unavailability or inaccuracy directly disrupts live trading operations or causes immediate financial exposure.

> Real-time inventory availability, live pricing rules, and in-flight order records — errors here cause incorrect fulfilment, phantom stock, or pricing compliance breaches.

### ReportingCritical
Data required for statutory financial reporting, regulatory submission, or formal performance management cycles.

> Daily sales reconciliation, VAT reporting feeds, and gross margin data used for periodic financial close.

### Operational
Data that supports day-to-day business processes where errors are recoverable without immediate trading disruption.

> Replenishment recommendations, store labour schedules, and supplier purchase orders where a short delay or correction does not halt trading.

### Reference
Supporting reference and configuration data where inaccuracies have limited operational consequence and can be corrected in the next refresh cycle.

> Postal code lookup tables, currency conversion rates, and product attribute taxonomies used to enrich reporting dimensions.

---

## Lifecycle
*Mutually exclusive*

### Active
Data is current, produced by live systems, and consumed by active business processes.

> Current season product catalogue, live customer accounts, and open order records being processed through the fulfilment pipeline.

### Discontinued
Data is no longer produced or updated but is retained and accessible for historical analysis and comparison.

> Product records for lines that have been ranged out, and transaction history from closed store locations.

### Archived
Data has been moved to low-cost storage and is accessible only through a formal retrieval process.

> Sales history older than the operational retention window, held for audit purposes but excluded from standard reporting environments.

### Deprecated
Data asset is scheduled for decommissioning; consumers have been formally notified and are expected to have migrated.

> Legacy product hierarchy tables being replaced by the new unified product master; decommission date published to all consuming teams.

---

## Retention
*Mutually exclusive*

### TransactionRecord
Financial transaction data subject to statutory tax authority and audit retention obligations.

> Itemised sales receipts, refund records, and payment settlement data required for VAT, corporation tax, and external audit purposes.

### CustomerConsentBound
Personal data retained only for as long as the customer's consent or legitimate interest basis remains valid under applicable data protection law.

> Customer profiles, loyalty balances, and purchase history where processing relies on consent or a legitimate interest assessment that must be reviewed at renewal or on withdrawal.

### OperationalLog
Operational records retained for internal audit, dispute resolution, or supplier reconciliation purposes.

> Goods-received notes, stock adjustment logs, and carrier tracking events retained to support claims and inventory discrepancy investigations.

### PromotionalCampaign
Marketing and campaign performance data retained through one complete trading cycle to support post-campaign evaluation and year-on-year comparison.

> Promotional uplift data, campaign response rates, and voucher redemption records retained for one full trading year before deletion.

---

## QualityTrust
*Mutually exclusive*

### Certified
Data has passed both automated validation and manual review; suitable for regulatory reporting and financial decision-making.

> Reconciled daily sales figures signed off by finance, and inventory counts verified against a physical stock audit.

### Validated
Data has passed automated quality checks; suitable for operational use but not yet cleared for statutory reporting.

> Intraday sales feeds that have passed range and referential integrity checks but are pending end-of-day financial reconciliation.

### Indicative
Data has passed basic ingestion checks but not full validation; suitable for trend analysis and directional insight only.

> Preliminary demand signals from loyalty card swipes used to inform replenishment estimates before full transaction data is available.

### Unverified
Data has just been ingested or generated and has not yet undergone any quality assessment.

> Raw supplier price files received via EDI awaiting automated price-list validation before being compared against existing cost records.

---

## ComplianceLegal
*Multi-select*

### PCIDSSScope
Data subject to Payment Card Industry Data Security Standard controls and audit requirements.

> Cardholder data captured through payment terminals and online checkout flows; subject to tokenisation, encryption, and quarterly PCI audit scope.

### DataProtectionRegulated
Data subject to GDPR, CCPA, or equivalent consumer data protection legislation, requiring lawful basis, retention limits, and subject-rights handling.

> Customer personal data collected through loyalty programmes, online accounts, and marketing consent flows.

### FinancialReportingObligation
Data required for statutory financial reporting, tax compliance, or external audit, and subject to record-keeping obligations.

> Sales ledger data, VAT accounting records, and gross margin calculations supporting annual statutory accounts.

### SupplierContractual
Data governed by supplier or marketplace partner agreements that impose restrictions on use, sharing, or retention.

> Wholesale cost prices, supplier rebate structures, and marketplace seller data governed by contractual non-disclosure and usage restriction clauses.

### Standard
No specific regulatory or contractual obligation; governed by general internal data handling policy only.

> Internal store operational metrics, anonymised footfall counts, and staff scheduling data without personal identifiers.
