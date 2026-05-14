# Flat Quality Rules

Run file-oriented checks:

- controlled value candidate detection
- nullable but never null inference
- format inconsistency across sampled rows
- numeric range anomalies (negative quantity/price patterns)
- delete-management signal inference
- late-arriving data inference from date columns
- timezone token consistency (`Z`, offsets, named TZ)

Flag sampling limitations when file size requires partial reads.
