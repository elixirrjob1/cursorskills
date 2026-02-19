# API Quality Rules

For generic APIs, run inference-based checks:

- controlled value candidates from low-cardinality attributes
- nullability from payload samples
- format consistency for emails/phones/dates
- delete-management signals from flags and status values
- late-arrival estimation from event timestamps vs ingestion timestamps
- timezone normalization based on explicit offsets/timezone tokens

Mark confidence lower than direct DB metadata checks when inference is sample-based.
