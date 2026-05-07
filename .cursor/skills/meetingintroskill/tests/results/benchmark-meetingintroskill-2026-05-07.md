# Benchmark Report: meetingintroskill
_Generated: 2026-05-07 (incremental failed-only + full benchmark refresh)_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | ❌ FAIL |
| Unit Tests | 11 / 11 passed (100%) |
| Assertions | 37 / 37 passed (100%) |
| Categories | 10 / 13 passed (77%) |
| High Failures | 1 |
| Medium Failures | 3 |
| Comparator | — (SKILL.md unchanged vs prior review; Step 3c skipped) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Clear Acme Corp discovery call | should-trigger | ✅ PASS ↩ carried | 4/4 |
| 2 | Vague "Need an opener for a meeting" | should-trigger | ✅ PASS ↩ carried | 3/3 |
| 3 | GreenPath Solutions ambiguous company | should-trigger | ✅ PASS ↩ carried | 2/2 |
| 4 | Manufacturing kickoff — Head of Procurement | should-trigger | ✅ PASS ↩ carried | 3/3 |
| 5 | CHRO UK fintech — handoff line | should-trigger | ✅ PASS ↩ carried | 2/2 |
| 6 | Follow-up email (should not trigger) | should-not-trigger | ✅ PASS ↩ carried | 2/2 |
| 7 | Pitch deck (should not trigger) | should-not-trigger | ✅ PASS ↩ carried | 2/2 |
| 8 | Job interview Deloitte (should not trigger) | should-not-trigger | ✅ PASS ↩ carried | 2/2 |
| 9 | JPMorgan Chase — well-known bank inference | edge-case | ✅ PASS ↩ carried | 3/3 |
| 10 | Two-year client quarterly review scope | edge-case | ✅ PASS ↩ carried | 2/2 |
| 11 | Internal all-hands scope | edge-case | ✅ PASS | 2/2 |

## Assertion Detail (Eval 11 re-graded — incremental)

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|---------|
| 11 | Flags first external client meeting scope | ✅ | Frames playbook as opening a first meeting with a **new external client** vs internal all-hands |
| 11 | Asks how to proceed or offers options | ✅ | Explicit question plus Option A/B; withholds five intros until user chooses |

_All other assertions carried from prior graded run (unchanged; not re-listed)._

## Category Grades

| # | Category | Grade |
|---|----------|-------|
| 1 | Triggering | ✅ PASS ↩ carried |
| 2 | Anatomy & Structure | ✅ PASS ↩ carried |
| 3 | Instructions Clarity | ✅ PASS ↩ carried |
| 4 | Output Quality | ✅ PASS ↩ carried |
| 5 | Testability | ✅ PASS ↩ carried |
| 6 | Resource Efficiency | ✅ PASS ↩ carried |
| 7 | Security & Trust | ✅ PASS ↩ carried |
| 8 | Coexistence & Recall | ✅ PASS ↩ carried |
| 9 | Model Compatibility | ❌ FAIL |
| 10 | Workflow & Feedback Loops | ✅ PASS ↩ carried |
| 11 | Maintainability & Lifecycle | ❌ FAIL |
| 12 | Gotchas / Lessons Learned | ❌ FAIL |
| 13 | Anti-Pattern Audit | ✅ PASS ↩ carried |

## Version Comparison (Step 3c)

Not run — `SKILL.md` last modified 2026-04-30; no post-review skill edit.

## History (all reviews)

| When | Repo @ review | Unit | Assert | Cat | Verdict | Note |
|------|---------------|------|--------|-----|---------|------|
| 2026-05-07 AM | c4360c0 | 10/11 | 36/37 | 10/13 | FAIL | GreenPath inference fail |
| 2026-05-07 rerun | 6cb76aa | 10/11 | 36/37 | 10/13 | FAIL | GreenPath PASS; Eval 11 proceed-options fail |
| 2026-05-07 incremental | f0f39f0 | 11/11 | 37/37 | 10/13 | FAIL | Eval 11 PASS; governance cats unchanged |
