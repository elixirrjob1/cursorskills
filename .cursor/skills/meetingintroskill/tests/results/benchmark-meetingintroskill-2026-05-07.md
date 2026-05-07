# Benchmark Report: meetingintroskill
_Generated: 2026-05-07_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | ❌ FAIL |
| Unit Tests | 10 / 11 passed (91%) |
| Categories | 10 / 13 passed (77%) |
| High Failures | 1 |
| Medium Failures | 5 |
| Low Failures | 4 |

## Unit Test Results

| # | Test | Type | Result |
|---|------|------|--------|
| 1 | Clear Acme Corp discovery call | should-trigger | ✅ PASS |
| 2 | Vague "Need an opener for a meeting" | should-trigger | ✅ PASS |
| 3 | GreenPath Solutions ambiguous company | should-trigger | ❌ FAIL |
| 4 | Manufacturing kickoff — Head of Procurement | should-trigger | ✅ PASS |
| 5 | CHRO UK fintech — handoff line | should-trigger | ✅ PASS |
| 6 | Follow-up email (should not trigger) | should-not-trigger | ✅ PASS |
| 7 | Pitch deck (should not trigger) | should-not-trigger | ✅ PASS |
| 8 | Job interview Deloitte (should not trigger) | should-not-trigger | ✅ PASS |
| 9 | JPMorgan Chase — well-known bank inference | edge-case | ✅ PASS |
| 10 | Two-year client quarterly review scope | edge-case | ✅ PASS |
| 11 | Internal all-hands scope | edge-case | ✅ PASS |

## Category Grades

| # | Category | Grade |
|---|----------|-------|
| 1 | Triggering (Description Quality) | ✅ PASS |
| 2 | Anatomy & Structure | ✅ PASS |
| 3 | Instructions Clarity | ✅ PASS |
| 4 | Output Quality | ✅ PASS |
| 5 | Testability | ✅ PASS |
| 6 | Resource Efficiency | ✅ PASS |
| 7 | Security & Trust | ✅ PASS |
| 8 | Coexistence & Recall | ✅ PASS |
| 9 | Model Compatibility | ❌ FAIL |
| 10 | Workflow & Feedback Loops | ✅ PASS |
| 11 | Maintainability & Lifecycle | ❌ FAIL |
| 12 | Gotchas / Lessons Learned | ❌ FAIL |
| 13 | Anti-Pattern Audit | ✅ PASS |

## History (all reviews)

| Date | Unit Tests | Pass Rate | Categories | Pass Rate | Verdict | High Failures |
|------|-----------|-----------|------------|-----------|---------|---------------|
| 2026-05-07 | 10 / 11 | 91% | 10 / 13 | 77% | ❌ FAIL | Separation of duties |
