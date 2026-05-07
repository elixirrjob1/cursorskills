# Benchmark Report: meetingintroskill
_Generated: 2026-05-07 (rerun — same day)_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | ❌ FAIL |
| Unit Tests | 10 / 11 passed (91%) |
| Assertions | 36 / 37 passed (97%) |
| Categories | 10 / 13 passed (77%) |
| Comparator | — (SKILL.md unchanged since prior review) |
| High Failures | 1 |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Clear Acme Corp discovery call | should-trigger | ✅ PASS | 4/4 |
| 2 | Vague "Need an opener for a meeting" | should-trigger | ✅ PASS | 3/3 |
| 3 | GreenPath Solutions ambiguous company | should-trigger | ✅ PASS | 2/2 |
| 4 | Manufacturing kickoff — Head of Procurement | should-trigger | ✅ PASS | 3/3 |
| 5 | CHRO UK fintech — handoff line | should-trigger | ✅ PASS | 2/2 |
| 6 | Follow-up email (should not trigger) | should-not-trigger | ✅ PASS | 2/2 |
| 7 | Pitch deck (should not trigger) | should-not-trigger | ✅ PASS | 2/2 |
| 8 | Job interview Deloitte (should not trigger) | should-not-trigger | ✅ PASS | 2/2 |
| 9 | JPMorgan Chase — well-known bank inference | edge-case | ✅ PASS | 3/3 |
| 10 | Two-year client quarterly review scope | edge-case | ✅ PASS | 2/2 |
| 11 | Internal all-hands scope | edge-case | ❌ FAIL | 1/2 |

## Assertion Detail (rerun grader)

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|---------|
| 1 | Contains all 5 strategy labels | ✅ | Numbered 1–5: Relationship, Purpose, Curiosity, Proof light, Time & flow |
| 1 | No clarifying question before intros | ✅ | Context locked → Five intros |
| 1 | Recommendation naming one strategy | ✅ | Recommends Purpose + Curiosity runner-up |
| 1 | Greeting element on each intro | ✅ | Each starts with "Hi, I'm [Name]" |
| 2 | At least one question mark | ✅ | Multiple numbered asks |
| 2 | No full five-strategy intros yet | ✅ | Clarification-only response |
| 2 | Asks audience, tone, or industry | ✅ | Who's in room, tone, org/industry |
| 3 | Asks about industry or field | ✅ | "Industry / what they do — In one line, what does GreenPath do" |
| 3 | No 5 strategy labels before confirmed | ✅ | No intros yet |
| 4 | All 5 strategy labels | ✅ | Relationship through Time & flow |
| 4 | Recommendation section | ✅ | Recommends Purpose |
| 4 | Winner mentions procurement or manufacturing | ✅ | "Head of Procurement and category managers in manufacturing" |
| 5 | All 5 strategy labels | ✅ | Bridges labeled Relationship … Time & flow |
| 5 | After-intro guidance | ✅ | Handoff + pairing Curiosity/Purpose |
| 6 | No five openers as main deliverable | ✅ | Declines skill; requests email inputs |
| 6 | Email-related content | ✅ | States not applying skill; offers email draft |
| 7 | No five openers | ✅ | Declines skill |
| 7 | Deck/presentation content | ✅ | Asks for product, audience, outcome |
| 8 | No five client openers | ✅ | Declines skill |
| 8 | Interview advice | ✅ | Deloitte interview framing |
| 9 | No industry confirm question | ✅ | Locks banking/financial services |
| 9 | All 5 labels | ✅ | Five numbered intros |
| 9 | Finance/banking/risk context | ✅ | JPMC risk, regulated banking |
| 10 | Flags first-meeting vs recurring | ✅ | Scope flag for QBR vs first meeting |
| 10 | Not silent first-meeting intros | ✅ | Flag precedes QBR-adapted set |
| 11 | Flags external-client scope | ✅ | Scope flag for internal all-hands |
| 11 | Asks how to proceed or offers options | ❌ | Five internal intros follow flag without explicit fork |

## Category Grades

| # | Category | Grade |
|---|----------|-------|
| 1 | Triggering | ✅ PASS |
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

| When | Repo @ review | Unit | Assert | Cat | Verdict | Note |
|------|---------------|------|--------|-----|---------|------|
| 2026-05-07 AM | c4360c0 | 10/11 | 36/37 | 10/13 | FAIL | GreenPath inference fail |
| 2026-05-07 rerun | 6cb76aa | 10/11 | 36/37 | 10/13 | FAIL | GreenPath PASS; internal all-hands proceed-options fail |
