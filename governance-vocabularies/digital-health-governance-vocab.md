# Digital Health — Data Governance Vocabulary

---

## Architecture
*Mutually exclusive*

### Source
Data as received from clinical systems, patient-facing applications, medical devices, or health information exchanges — no transformations applied.

> Raw HL7 FHIR bundles from EHR exports, unprocessed wearable device payloads, and bulk data extracts from hospital information systems before any normalisation.

### Normalised
Data mapped to a canonical clinical data model — SNOMED CT codes applied, units standardised, identifiers resolved to platform entities.

> Patient encounters translated to a common FHIR R4 profile; medication records mapped to RxNorm; duplicate patient records resolved to a master patient index.

### Analytical
Aggregated, derived, or de-identified datasets structured for population health analysis, research, or care management reporting.

> Condition prevalence cohorts, readmission risk scores, and anonymised research datasets extracted from the normalised layer for approved studies.

---

## Privacy
*Mutually exclusive*

### Directly Identified PHI
Protected Health Information containing direct identifiers — name, date of birth, contact details, or other HIPAA 18 identifiers present and intact.

> Full EHR records, prescription histories, and appointment records linked to an individual patient by name or national health identifier.

### Pseudonymised PHI
PHI where direct identifiers have been replaced with a reversible token; re-identification is possible with the linkage key held in a separate controlled vault.

> Clinical trial records tokenised to a subject ID; the patient-to-subject mapping is held by the site coordinator under access control.

### De-identified
Data from which the HIPAA Safe Harbor or Expert Determination criteria have been satisfied; re-identification risk is assessed as negligible.

> Discharge summaries with all 18 HIPAA identifiers removed, geographic detail reduced to three-digit ZIP, and dates shifted by a consistent random offset per patient.

### Anonymised / Synthetic
Data that either has been irreversibly anonymised such that re-identification is not feasible, or has been synthetically generated to replicate statistical properties without representing real individuals.

> Synthetic patient cohorts generated for algorithm validation; k-anonymised population datasets approved for open research publication.

### Non-Personal Clinical Reference
Clinical vocabulary, protocol, or reference data with no patient linkage.

> ICD-10 code tables, clinical pathway definitions, drug interaction reference data, and care quality indicator benchmarks.

---

## Criticality
*Mutually exclusive*

### Clinical Decision Support
Data actively used to inform or automate a point-of-care clinical decision; errors could cause direct patient harm.

> Real-time allergy flags, medication interaction alerts, and sepsis early-warning scores surfaced in the clinical workflow.

### Clinical Record
The official patient health record; accuracy and availability are required for care continuity and medico-legal accountability.

> Problem lists, medication orders, procedure notes, and diagnostic results held in the EHR as the source of truth for ongoing care.

### Research and Population Health
Data used for retrospective analysis, epidemiological study, or quality improvement — not directly involved in individual patient care.

> Cohort datasets used for disease burden studies, care pathway effectiveness evaluations, and public health surveillance.

### Administrative and Operational
Data supporting scheduling, billing, capacity planning, or compliance reporting with no direct clinical function.

> Appointment slot utilisation, claims submission records, insurance eligibility data, and workforce allocation metrics.

---

## Lifecycle
*Mutually exclusive*

### Active Clinical
Data associated with a patient who is currently receiving care; subject to real-time update and immediate access requirements.

> Open inpatient encounters, active medication orders, and ongoing care plans for admitted or recently discharged patients.

### Retrospective
Data for patients no longer in an active care episode; retained for care continuity, audit, and legal purposes.

> Completed encounter records and historical medication histories for patients with no open episodes, within the statutory retention window.

### Research Archived
De-identified or consented research data retained beyond the clinical retention period for longitudinal study purposes.

> Cohort data held under a research governance approval for the duration of an approved study, potentially beyond standard clinical retention.

### Destroyed
Data deleted following expiry of all applicable retention obligations and confirmation that no legal hold is in force.

> Patient records deleted after the full statutory and organisational retention period has elapsed and destruction has been approved by the records manager.

---

## Retention
*Mutually exclusive*

### Statutory Clinical Minimum
Retention period prescribed by healthcare law or regulation — deletion before this period is prohibited.

> Adult patient records retained for a minimum of eight years post last contact under UK NHS guidelines; paediatric records retained until the patient's 25th birthday.

### Extended Research Consent
Retained beyond statutory minimums under the terms of a patient's informed research consent.

> Biobank samples and linked EHR data retained for up to 30 years per consent form, subject to annual ethics review.

### Operational
Retained at the organisation's discretion while the data supports active clinical or administrative processes.

> Appointment scheduling histories and administrative correspondence retained until they are no longer needed for operational reference.

### Transient Processing
Data that must be deleted immediately after its processing purpose is complete — must not be persisted to durable storage.

> Intermediate identifiers used during a de-identification pipeline run; PHI staging records that must be purged once the anonymised output is confirmed.

---

## QualityTrust
*Mutually exclusive*

### Validated Clinical
Data reviewed and attested by a credentialled clinician or validated against an authoritative clinical source.

> Physician-signed diagnostic reports, pharmacist-verified medication reconciliation records, and pathology results confirmed by a consultant.

### System-Generated Unvalidated
Data produced automatically by a clinical system or device without clinician review or sign-off.

> Continuous vital sign streams from bedside monitors, wearable step counts, and auto-populated structured data fields in the EHR prior to clinician attestation.

### Derived / Computed
Data calculated from validated clinical inputs — accuracy depends on the quality of the source data and the validity of the algorithm.

> Risk scores, predicted outcomes, and population health metrics computed by analytical models or clinical decision support engines.

### Patient-Reported
Data entered directly by the patient — self-reported symptom scores, lifestyle questionnaires, and patient-generated health data from apps.

> PRO (Patient-Reported Outcome) questionnaire responses, self-measured blood pressure readings submitted via a patient portal, and symptom diary entries.

---

## ComplianceLegal
*Multi-select*

### HIPAA Covered
Data constituting Protected Health Information under the US Health Insurance Portability and Accountability Act — Privacy Rule and Security Rule obligations apply.

> Any individually identifiable health information created, received, or maintained by a HIPAA covered entity or business associate.

### GDPR Health Data (Special Category)
Data classified as health data under GDPR Article 9 — requires an explicit legal basis, a Data Protection Impact Assessment, and appropriate safeguards.

> Patient records processed by EU-based health providers or any data transferred to the EU from third countries without an adequate transfer mechanism.

### Research Ethics Approved
Data collected or processed under a formal ethics board approval — use is restricted to the approved study protocol and consent scope.

> Clinical trial data, biobank specimens, and linked EHR extracts governed by an IRB or NHS Research Ethics Committee approval.

### Medico-Legal Hold
Data subject to a legal hold in connection with active or anticipated litigation, regulatory investigation, or coroner's inquiry.

> Patient records flagged as relevant to a clinical negligence claim or a serious incident investigation; deletion is suspended for the duration of the hold.

### Unrestricted Reference
Clinical reference and vocabulary data with no patient linkage and no regulatory constraint.

> SNOMED CT, ICD-10, LOINC, and formulary datasets licensed for internal clinical use with no personal data content.
