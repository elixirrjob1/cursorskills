# PostgreSQL Quality Rules

The PostgreSQL route runs full quality checks currently implemented by the analyzer:

- controlled value candidates
- nullable but never null
- missing primary keys
- missing foreign keys with orphan detection
- format inconsistency
- range violations
- delete management
- late-arriving data
- timezone analysis

Treat missing PK as critical. Treat missing FK as warning unless orphan rate is significant.
