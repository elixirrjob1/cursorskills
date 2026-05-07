# HR Data — Data Governance Vocabulary

---

## Architecture
*Mutually exclusive*

### Source Extract
Raw HR data as exported from the system of record — HRIS, payroll engine, ATS, or performance management platform — before any transformation or quality enforcement.

> Unmodified employee master data exports from the HRIS, raw payroll run files, and ATS application records as received from operational systems.

### Integrated
Data from multiple HR source systems that has been reconciled, identity-resolved, and conformed to the organisation's canonical HR data model.

> Employee records assembled from HRIS, time-and-attendance, and benefits platforms; organisational hierarchy resolved to a single authoritative structure.

### Analytical
Aggregated, derived, or anonymised HR data structured for workforce analytics, planning, or executive reporting — not sourced directly from operational systems.

> Attrition rate trends, headcount forecasts, anonymised pay equity analyses, and workforce composition metrics prepared for People Analytics dashboards.

---

## Privacy
*Mutually exclusive*

### Sensitive Category Personal Data
HR data falling under GDPR Article 9 special categories — health, disability, trade union membership, biometric, or belief-related data about an individual employee.

> Occupational health records, disability accommodation requests, trade union membership declarations, and biometric time-attendance records.

### Standard Personal Data
Individually identifiable HR data that does not fall into a special category but is subject to full GDPR personal data obligations.

> Name, job title, salary, employment history, performance ratings, and contact details held in the employee master record.

### Pseudonymised
HR data where the direct employee identifier has been replaced with a token — re-identification requires access to a separately controlled mapping table.

> Anonymised employee IDs used in pay equity analytics and engagement survey datasets where individual responses are linked to a token rather than a name.

### Anonymised Aggregate
Statistical HR data from which individual re-identification is not feasible — no personal data obligations apply.

> Headcount by department and grade, voluntary attrition rates by tenure band, and gender pay gap statistics reported at an organisational level.

---

## Criticality
*Mutually exclusive*

### Payroll Critical
Data whose unavailability or inaccuracy would cause employees not to be paid correctly or on time.

> Active employee pay elements, bank account details, tax codes, and deduction instructions — errors directly affect individuals' financial wellbeing and trigger legal liability.

### Compliance Mandatory
Data required to fulfil a statutory employment or labour law obligation — inaccuracy or unavailability constitutes a legal breach.

> Right-to-work verification records, working time directive compliance logs, statutory leave entitlement records, and gender pay gap reporting inputs.

### Operational HR
Data supporting routine HR processes — recruitment, onboarding, performance, and absence management — where errors cause process inefficiency but not immediate legal exposure.

> Interview scorecards, onboarding task completion records, and informal performance check-in notes.

### Workforce Intelligence
Data used for strategic planning, analytics, or executive reporting — unavailability affects decision quality but not immediate operational function.

> Succession pipeline assessments, workforce demand forecasts, and voluntary attrition predictive model outputs.

---

## Lifecycle
*Mutually exclusive*

### Active Employment
Data relating to a current employee — actively updated and subject to full operational and compliance obligations.

> Live employee master records, current compensation structures, and active benefits enrolments for individuals currently on the payroll.

### Post-Separation
Data relating to an employee who has left the organisation — updates are limited to corrections; data is retained for legal and audit purposes.

> Leaver records, final payroll runs, exit interview responses, and reference provision logs for former employees within the statutory retention window.

### Historical
Data retained beyond the post-separation window for long-term trend analysis, legal hold, or pension administration.

> Salary history used for pension benefit calculations, long-service records needed for employment tribunal defence, and historical organisational structure snapshots.

### Purged
Data deleted in accordance with the approved retention schedule, confirmed across all systems and backups.

> Personal data erased following expiry of all applicable retention periods or in response to a verified GDPR erasure request.

---

## Retention
*Mutually exclusive*

### Statutory Employment Law
Retention period prescribed by employment legislation, tax authority requirements, or labour regulation — deletion before expiry is prohibited.

> Payroll records retained for the statutory minimum under HMRC / IRS requirements; right-to-work documents retained for the post-termination period prescribed by immigration law.

### Legal Hold
Retention suspended pending the resolution of an employment dispute, tribunal claim, or regulatory investigation.

> Personnel files, correspondence, and performance records for individuals named in an active or anticipated employment tribunal claim.

### Operational
Retained while the data supports an active HR process — no statutory obligation, deleted when the business purpose lapses.

> Recruitment pipeline data for positions that have been filled; probationary review notes after the probation period is formally closed.

### Transient
Data that must be deleted immediately after its processing purpose is complete and must not be persisted.

> Interim working copies created during a bulk data migration, and de-identification pipeline staging records that must be purged once the anonymised output is confirmed.

---

## QualityTrust
*Mutually exclusive*

### System of Record
Data produced and maintained by the authoritative HR system for this information type — the trusted source for downstream consumers.

> Employee master data in the HRIS, payroll figures from the payroll engine, and approved organisational hierarchy from the workforce management platform.

### Reconciled
Data validated against the system of record — discrepancies have been identified and resolved before downstream use.

> Headcount figures cross-checked between HRIS and Finance GL; time-and-attendance data reconciled against payroll inputs at period close.

### Derived
Computed from system-of-record inputs — accuracy depends on the quality of source data and the validity of the calculation logic.

> Annualised salary equivalents, full-time-equivalent headcount, and predicted attrition scores produced by the People Analytics model.

### Self-Reported
Data entered directly by the employee without independent verification — accuracy and completeness depend on individual disclosure.

> Voluntary diversity declaration responses, emergency contact details, and self-certified absence records submitted through the employee self-service portal.

---

## ComplianceLegal
*Multi-select*

### GDPR Standard Personal Data
HR personal data subject to GDPR — lawful basis documented (typically contractual necessity or legal obligation), data subject rights applicable.

> Employee contact details, employment history, performance records, and payroll data processed under the employment contract or statutory obligation.

### GDPR Special Category
HR data falling under GDPR Article 9 — requires explicit consent or a specific legal basis, enhanced security and access controls, and a Data Protection Impact Assessment.

> Disability records, health and sickness absence data, trade union membership, and biometric data — must not be processed without a documented Article 9 basis.

### Employment Law Regulated
Data whose collection, retention, or disclosure is governed by employment legislation, labour codes, or works council agreements.

> Right-to-work documentation, working time records, payslip data, and collective agreement compliance records — breach carries statutory penalty.

### Contractually Restricted
Data whose use or sharing is constrained by an employment contract clause, settlement agreement, or third-party HR service agreement.

> Non-disclosure obligations in settlement agreements; data processed by an outsourced payroll provider under a data processing agreement with defined permitted uses.

### Unrestricted Internal
No regulatory, legal, or contractual constraint applies — access is limited by internal data classification policy rather than external obligation.

> Fully anonymised workforce statistics, published organisational charts, and non-personal HR process documentation.
