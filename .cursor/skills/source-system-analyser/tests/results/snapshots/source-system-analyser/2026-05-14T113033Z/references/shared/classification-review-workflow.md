# Classification Review Workflow

## When To Use This

Use this workflow when a generated `schema.json` has:

- `concept_id: null`
- low `concept_confidence`
- an obviously wrong concept
- a large schema where you need to improve classification over multiple runs

This is the normal way to improve classification quality on a large database. Do not try to perfect every field in one pass.

## What Null Means

`null` usually means the classifier did not have enough evidence to safely assign a concept.

That is often better than a wrong classification. Treat `null` as a review queue:

- some `null` fields are high-value and worth fixing
- some `null` fields are low-value free text and can stay unclassified
- the goal is not 100% classification; the goal is useful and reliable classification

## The Review Loop

Use this loop each time you improve the rules:

1. Run the analyzer and generate a fresh `schema.json`.
2. Open the latest output in Cursor.
3. Group fields into three buckets:
   - `concept_id: null`
   - low confidence, for example `< 0.60`
   - obvious false positives
4. Pick one repeated family of fields at a time.
5. Inspect the evidence for that family:
   - `concept_evidence`
   - `concept_confidence`
   - `concept_alias_group`
   - table name and table context
   - sample values
   - join candidates and foreign keys
6. Make the smallest reusable rule change.
7. Rerun the analyzer.
8. Compare before and after on the same field family.

The key discipline is: fix one family, rerun, then review. Do not change many unrelated rules at once.

## What To Fix First

Use this priority order:

1. False positives
2. High-value nulls
3. Repeated business patterns
4. Low-value edge cases

### 1. False Positives

These come first. A wrong concept is more dangerous than `null`.

Examples:

- `product_description` classified as `network.ip_address`
- `state` classified as `entity.status`

### 2. High-Value Nulls

These are fields that matter for ingestion, joins, filtering, or business meaning:

- IDs and references
- timestamps and event dates
- money and amounts
- status/type/category fields
- contact fields

Examples:

- `store_id`
- `last_restocked_at`
- `role`

### 3. Repeated Business Patterns

Once obvious errors are removed, look for repeated families:

- `dept_code`, `region_code`, `store_code`
- `created_on`, `insert_ts`, `last_seen_at`
- `customer_email`, `email_address`, `primary_email`

When a pattern repeats, add one reusable rule instead of fixing one field at a time.

### 4. Low-Value Edge Cases

These can stay `null` unless there is a real reason to classify them:

- descriptions
- comments
- notes
- long free text

## Safe Order Of Rule Changes

Use this order when improving classification:

1. Alias additions
2. Concept name token additions
3. Value-pattern detectors
4. Table-context hints
5. Scoring weight changes

Why this order works:

- alias changes are narrow and easy to validate
- concept token changes are still fairly local
- value detectors can affect many columns
- table-context hints are broader and should be used carefully
- scoring changes can shift the whole system and should be last

In general, prefer additive changes over global changes.

## Cursor Prompts

Use prompts like these directly in Cursor.

### Find The Review Queue

```text
Review this generated schema JSON. Show me all columns where concept_id is null or concept_confidence is below 0.60. Group them by likely pattern and tell me which family I should fix first.
```

### Explain Why A Group Is Null

```text
For this group of null classifications, explain why the classifier did not assign a concept. Use concept_evidence, sample values, table names, and join candidates.
```

### Find Repeated Aliases

```text
Find repeated aliases in this schema where one column is classified correctly and similar columns are null or low-confidence.
```

### Suggest The Smallest Safe Rule Change

```text
Suggest the smallest safe rule change to classify these columns consistently without increasing false positives.
```

### Compare Two Runs

```text
Compare the previous and current schema JSON outputs and tell me which classifications improved, regressed, or stayed null.
```

### Review False Positives Only

```text
Review only false positives in this schema and tell me which rule is most likely causing each one.
```

### Prioritize By Business Value

```text
From the null and low-confidence classifications in this schema, separate high-value fields from low-value fields. Prioritize IDs, timestamps, money, status/type/category, and contact fields.
```

## Example Walkthrough

### Example 1: `last_restocked_at` Is Null

Symptom:

- `last_restocked_at` comes back with `concept_id: null`
- evidence shows a datetime type and timestamp-like sample values

What to inspect:

- `concept_evidence`
- column name tokens
- sample values
- whether similar fields like `created_at` or `event_time` already classify correctly

Likely fix:

- add a temporal event alias or token such as `restocked_at` or `last_restocked_at`

Expected outcome:

- the next run should classify it as `temporal.event_time`

### Example 2: `product_description` Is A False Positive

Symptom:

- `product_description` is classified as something unrelated

What to inspect:

- the name-token rule that matched
- whether a short substring is matching inside a larger token
- whether there was any value evidence supporting the concept

Likely fix:

- tighten the token matching rule so short tokens do not match arbitrary substrings

Expected outcome:

- the field should become `null` unless there is strong evidence for a real concept

### Example 3: `store_id` Or `product_id` Is Weak Or Null

Symptom:

- an ID field is weakly classified or unclassified
- the table has join candidates or explicit foreign keys

What to inspect:

- `join_candidates`
- `foreign_keys`
- concept evidence for identifier concepts

Likely fix:

- add or strengthen structural scoring for FK-like columns instead of only using name tokens

Expected outcome:

- columns with real structural evidence should move toward `identifier.foreign_key`

## How To Use The Registry

The top-level `concept_registry` helps you decide what to improve next.

Look for:

- concepts with very few columns where you expected more
- concepts with low average confidence
- large numbers of nulls in a business area that should be recognized
- over-broad concepts that are capturing unrelated fields

The registry is useful for spotting system-wide gaps, while `classification_summary` is useful for reviewing one table at a time.

## Stopping Criteria

You do not need to force every column into a concept.

A classification pass is usually good enough when:

- obvious false positives are gone
- high-value fields are classified well
- repeated critical concepts are stable across tables
- remaining nulls are mostly low-value or genuinely ambiguous
- confidence is high on the concepts you rely on for ingestion and analysis

If the remaining unclassified fields are mostly descriptions, notes, or one-off business fields, it is usually better to stop than to overfit the rules.
