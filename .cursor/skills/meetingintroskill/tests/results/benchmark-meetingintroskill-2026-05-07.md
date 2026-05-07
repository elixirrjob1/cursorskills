# Benchmark Report: meetingintroskill
_Generated: 2026-05-07_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | ❌ FAIL |
| Unit Tests | 10 / 11 passed (91%) |
| Assertions | 36 / 37 passed (97%) |
| Categories | 10 / 13 passed (77%) |
| High Failures | 1 |
| Medium Failures | 5 |
| Low Failures | 4 |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Clear Acme Corp discovery call | should-trigger | ✅ PASS | 4/4 |
| 2 | Vague "Need an opener for a meeting" | should-trigger | ✅ PASS | 3/3 |
| 3 | GreenPath Solutions ambiguous company | should-trigger | ❌ FAIL | 1/2 |
| 4 | Manufacturing kickoff — Head of Procurement | should-trigger | ✅ PASS | 3/3 |
| 5 | CHRO UK fintech — handoff line | should-trigger | ✅ PASS | 2/2 |
| 6 | Follow-up email (should not trigger) | should-not-trigger | ✅ PASS | 2/2 |
| 7 | Pitch deck (should not trigger) | should-not-trigger | ✅ PASS | 2/2 |
| 8 | Job interview Deloitte (should not trigger) | should-not-trigger | ✅ PASS | 2/2 |
| 9 | JPMorgan Chase — well-known bank inference | edge-case | ✅ PASS | 3/3 |
| 10 | Two-year client quarterly review scope | edge-case | ✅ PASS | 2/2 |
| 11 | Internal all-hands scope | edge-case | ✅ PASS | 2/2 |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|---------|
| 1 | Contains all 5 strategy labels | ✅ | All 5 headers present: Relationship, Purpose, Curiosity, Proof light, Time & flow |
| 1 | No clarifying question before intros | ✅ | Opens with Context Locked → Five Openings, no questions |
| 1 | Includes recommendation naming one strategy | ✅ | "Winner: #3 — Curiosity" |
| 1 | Each intro begins with greeting element | ✅ | All 5 intros open with "Hi [Name]" |
| 2 | Contains at least one question mark | ✅ | "Who's in the room?", "What tone?", "What industry?" |
| 2 | Does NOT contain all 5 strategy labels | ✅ | None of the strategy labels appear; response is questions only |
| 2 | Asks about audience, tone, or industry | ✅ | Asks all three explicitly |
| 3 | Asks about industry or field | ❌ | Infers "environmental consulting, sustainability" from name; asks only about audience and style |
| 3 | Does NOT contain all 5 strategy labels before industry confirmed | ✅ | No strategy labels in response |
| 4 | Contains all 5 strategy labels | ✅ | Relationship, Purpose, Curiosity, Proof Light, Time & Flow all present |
| 4 | Includes recommendation section | ✅ | "Winner: #5 — Time & Flow" |
| 4 | Winner reasoning contains procurement or manufacturing | ✅ | "Procurement people are operationally wired" |
| 5 | Contains all 5 strategy labels | ✅ | All 5 bold numbered items present |
| 5 | Includes guidance on what to say after intro | ✅ | "What to Say Immediately After the Intro Lands" section with bridging line |
| 6 | Does NOT contain 5 strategy labels as meeting openers | ✅ | No strategy labels; skill decline noted |
| 6 | Contains email-related content | ✅ | "Subject: Following up from yesterday's call" + "Hi [Client Name]" |
| 7 | Does NOT produce 5 spoken meeting opener strategies | ✅ | 12-slide deck structure, no opener strategies |
| 7 | Addresses deck, slide, or presentation content | ✅ | Full 12-slide pitch deck structure |
| 8 | Does NOT produce 5 client meeting opener strategies | ✅ | Present→Past→Future interview framework, no client openers |
| 8 | Addresses interview or self-introduction advice | ✅ | "Tell me about yourself at Deloitte" with structure tips |
| 9 | Does NOT ask to confirm industry | ✅ | Immediately locks "Global investment banking / financial services" |
| 9 | Contains all 5 strategy labels | ✅ | All 5 present |
| 9 | Mentions finance, banking, or risk context | ✅ | "Global investment banking", "credit, market, operational, or model risk" |
| 10 | Flags skill designed for first meetings | ✅ | "Scope mismatch flagged" — explains skill is for new relationships |
| 10 | Does NOT silently produce first-meeting intros | ✅ | No opener strategies; halts and asks clarifying questions |
| 11 | Flags skill targets first external client meetings | ✅ | "Designed specifically for first external client meetings" |
| 11 | Asks how to proceed or offers options | ✅ | "How would you like to proceed?" with two explicit options |

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
