# IoT Sensor Data — Data Governance Vocabulary

---

## Architecture
*Mutually exclusive*

### Raw Signal
Unprocessed telemetry as emitted by the sensor and received by the ingestion layer — no filtering, aggregation, or quality correction applied.

> Millisecond-resolution readings from edge devices, including out-of-range values, transmission errors, and duplicate packets, preserved exactly as received.

### Processed
Data that has been cleaned, validated against calibration parameters, gap-filled, and aligned to a common time base.

> Temperature readings corrected against calibration coefficients, GPS tracks with outlier coordinates removed, and resampled vibration signals aligned to a uniform interval.

### Aggregated / Derived
Statistical summaries, event detections, or computed metrics derived from processed signals — optimised for analytical and operational consumption.

> Hourly energy consumption totals, anomaly detection event logs, predictive maintenance feature vectors, and equipment health indices.

---

## Privacy
*Mutually exclusive*

### Directly Identifying
Sensor data that identifies a natural person without additional linking — location, biometric, or behavioural data with direct person-to-device binding.

> GPS traces from personal vehicles, wearable biometric streams tied to a named individual, and smart home energy profiles linked to a residential occupant.

### Pseudonymised Personal
Sensor data linked to an individual via a device or account token — personal identity is not directly exposed but is recoverable via the device registry.

> Connected health device readings stored against a device ID whose owner mapping is held in a separate access-controlled registry.

### Aggregate Personal
Statistical summaries derived from personal sensor data where individual re-identification is not feasible.

> Average step counts by anonymised age cohort, building occupancy estimates from motion sensors, and anonymised route-level travel time statistics.

### Non-Personal Operational
Sensor data with no connection to a natural person — industrial equipment, environmental monitoring, or infrastructure sensors.

> Compressor pressure readings, pipeline flow rates, ambient temperature and humidity logs from weather stations, and power grid frequency measurements.

---

## Criticality
*Mutually exclusive*

### Safety Critical
Data used to detect or prevent conditions that could cause harm to people, equipment, or the environment — failure to act on this data has immediate safety consequences.

> Gas leak detection sensor readings, industrial machine over-temperature alerts, and medical device infusion rate monitors where an undetected fault could injure or kill.

### Process Critical
Data whose unavailability or inaccuracy would cause a significant production, operational, or quality failure — safety is not immediately at risk but business impact is severe.

> CNC machine tool wear sensors feeding automated tooling decisions, cold-chain temperature loggers for pharmaceutical shipments, and yield-critical quality inspection data.

### Condition Monitoring
Data used for predictive maintenance or asset health tracking — degraded availability increases maintenance risk over time but does not cause immediate failure.

> Vibration and acoustic emission sensors on rotating equipment, corrosion monitoring probes, and power quality meters used to schedule planned maintenance.

### Environmental and Diagnostic
Contextual sensor data used to characterise operating conditions or investigate historical events — no real-time operational dependency.

> Weather station readings used to contextualise energy consumption analysis, background radiation monitors, and retrospective fault investigation logs.

---

## Lifecycle
*Mutually exclusive*

### Active Deployment
Sensor is operational and data is being collected under current calibration certification — data is trusted for its intended purpose.

> In-service sensors within their certified calibration interval, actively reporting to the ingestion platform and included in operational dashboards.

### Calibration Due
Sensor is operational but its calibration certificate has expired or is approaching expiry — data should be treated with caution until recalibration is confirmed.

> Sensors past their scheduled calibration date; data collected during this period must be flagged as unconfirmed until a calibration audit is completed.

### Decommissioned
Sensor has been retired — historical data is retained for reference but no new data is produced.

> Replaced or removed sensors whose historical readings are retained for trend analysis, audit, or compliance purposes.

---

## Retention
*Mutually exclusive*

### Regulatory or Safety Mandated
Retention period prescribed by safety regulation, environmental law, or an industry-specific compliance obligation.

> Aviation sensor data retained under EASA airworthiness regulations; industrial emissions monitoring data retained under environmental permitting conditions.

### Operational Signal Window
Raw high-frequency signal data retained only for the window needed to support near-real-time processing and replay.

> Millisecond-resolution vibration streams retained for 72 hours to allow reprocessing in the event of a pipeline failure, then purged or downsampled.

### Aggregated Long-Term
Aggregated or downsampled derivatives retained for trend analysis, model training, or regulatory look-back beyond the raw signal window.

> Hourly equipment health indices retained for five years to support predictive model retraining and long-term asset performance benchmarking.

### Event-Triggered Extended
Data surrounding a detected anomaly, incident, or threshold breach retained beyond standard schedules pending investigation or legal hold.

> Sensor logs captured in the hour before and after a reported safety event, retained until the incident investigation is formally closed.

---

## QualityTrust
*Mutually exclusive*

### Calibration Certified
Data collected by a sensor within its valid calibration certificate period — accuracy within the manufacturer-specified tolerance is confirmed.

> Readings from sensors whose calibration was verified by an accredited laboratory within the required interval and whose certificates are on file.

### Uncalibrated
Data collected by a sensor with no current calibration certificate — accuracy bounds are unknown.

> Readings from newly deployed sensors awaiting first calibration, or sensors whose calibration status is unrecorded.

### Degraded Signal
Data from a sensor known to be operating outside its normal parameters — potential for systematic bias, noise, or gaps.

> Readings flagged by automated anomaly detection as exhibiting drift, excessive noise, or intermittent connectivity; must not be used for safety or regulatory purposes without expert review.

### Synthetic / Simulated
Data generated by a simulation model or signal generator — not produced by a physical sensor in the field.

> Simulated sensor feeds used for algorithm development, digital twin validation, and training data augmentation.

---

## ComplianceLegal
*Multi-select*

### GDPR Personal Data
Sensor data that constitutes personal data under GDPR — processing requires a documented lawful basis and data subject rights must be honoured.

> Location traces, biometric readings, and any sensor stream that can be linked to an identified or identifiable natural person.

### Safety Regulation Governed
Data whose collection, retention, and quality are mandated by a sector-specific safety authority or standard.

> Aviation flight recorder data governed by ICAO Annex 6; nuclear plant sensor logs governed by national nuclear regulatory bodies; medical device data governed by MDR/FDA requirements.

### Contractual Data Sharing
Data collected under a third-party data sharing agreement that restricts its use, transfer, or disclosure.

> Sensor data received from a fleet operator under a telematics data sharing agreement; building management sensor feeds provided by a facilities contractor under contract terms.

### Unrestricted
No regulatory, contractual, or personal data constraint applies — sensor data may be used and shared freely within the platform.

> Environmental monitoring stations producing public-domain air quality readings; publicly syndicated weather sensor feeds with no personal data content.
