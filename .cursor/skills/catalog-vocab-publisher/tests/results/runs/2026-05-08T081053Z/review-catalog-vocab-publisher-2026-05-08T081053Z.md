# Skill Review: catalog-vocab-publisher

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

This is an incremental re-check following the prior review (2026-05-07T140243Z). The critical `publish_vocab.py` auth mismatch (HIGH, Cat 7) has been fully resolved: the script now reads `OM_BASE_URL` and `OM_TOKEN` from environment variables, matching the SKILL.md workflow exactly. This resolves the prior HIGH failure in Security & Trust and upgrades Cat 7 to PASS. However, Cat 11 (Maintainability & Lifecycle) still fails: no peer-review or separation-of-duties requirement is documented in the skill, and no versioning strategy is defined. Until these governance gaps are addressed, the skill does not meet the production-ready bar. All other 11 categories remain PASS (carried from prior review). Unit test pass rate is unchanged at 10/13 — the three remaining failures (evals 2, 3, 7) are caused by the SKILL.md step-gating workflow: the agent correctly stops at Step 1 to confirm file path and env vars before showing the Step 2 command, but this prevents the command from appearing in the immediate response to already-specific prompts. This is a workflow design tradeoff, not a security issue.

## Unit Tests: 10 / 13 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Basic publish trigger | Should-trigger | ✅ PASS | |
| 2 | Named file push with JWT auth | Should-trigger | ❌ FAIL | Agent confirms env vars at Step 1 but doesn't show the publish command yet |
| 3 | Idempotent re-run | Should-trigger | ❌ FAIL | Agent correctly notes idempotency; stops at Step 1 without showing command |
| 4 | 401 error handling | Should-trigger | ✅ PASS | |
| 5 | 403 error handling | Should-trigger | ✅ PASS | |
| 6 | Upload framework | Should-trigger | ✅ PASS | |
| 7 | Push updated descriptions | Should-trigger | ❌ FAIL | Step 1 gating prevents UPDATED/SKIPPED explanation and command from appearing |
| 8 | Exact trigger phrase | Should-trigger | ✅ PASS | |
| 9 | Should-not-trigger: vocab generation | Should-not-trigger | ✅ PASS | ↩ carried |
| 10 | Should-not-trigger: Snowflake ingestion | Should-not-trigger | ✅ PASS | ↩ carried |
| 11 | Edge: no file path | Edge case | ✅ PASS | ↩ carried |
| 12 | Edge: service account not set up | Edge case | ✅ PASS | ↩ carried |
| 13 | Edge: SSL errors | Edge case | ✅ PASS | ↩ carried |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS | ✓ Name valid (23 chars), what/when/triggers all present, 5 explicit trigger phrases ↩ carried |
| 2 | Anatomy & Structure | PASS | ✓ Valid frontmatter, ~139 lines, references shallow (2 hops max) ↩ carried |
| 3 | Instructions Clarity | PASS | ✓ Step-by-step workflow, explicit 401/403 branches, consistent terminology ↩ carried |
| 4 | Output Quality | PASS | ✓ Example output block defines exact CREATED/UPDATED/SKIPPED format ↩ carried |
| 5 | Testability | PASS | ✓ Test suite present; all HIGH subcategories pass ↩ carried |
| 6 | Resource Efficiency | PASS | ✓ Pre-built script invoked deterministically; no over-explanation of common knowledge ↩ carried |
| 7 | Security & Trust | PASS | ✓ Re-evaluated: script now uses OM_TOKEN env var — auth mismatch resolved; no hardcoded credentials; no sensitive value logging |
| 8 | Coexistence & Recall | PASS | ✓ Specific triggers; LOW coexistence note still absent but doesn't fail category ↩ carried |
| 9 | Model Compatibility | PASS | ✓ `validated_on: Claude Sonnet 4.6` in frontmatter and Registry ↩ carried |
| 10 | Workflow & Feedback Loops | PASS | ✓ Confirm → run → review loop; 401/403 fix-and-retry paths present ↩ carried |
| 11 | Maintainability & Lifecycle | FAIL | [HIGH] No peer review / separation-of-duties documented; [MEDIUM] versioning strategy missing |
| 12 | Gotchas / Lessons Learned | PASS | ✓ 5 real-world gotchas documented ↩ carried |
| 13 | Anti-Pattern Audit | PASS | ✓ Explicit error handling; no voodoo constants; requirements.txt still absent (MEDIUM, 3/4 pass → 75% ≥ 70%) ↩ carried |

## High-Criticality Failures

### **Subcategory:** Separation of duties observed
- **Category:** Maintainability & Lifecycle (Cat 11)
- **Finding:** The Registry table lists "Data Platform team" as owner but contains no requirement for peer review before merging changes to this skill. No PR review process, approval gate, or reviewer role is documented. The skill-reviewer's own rubric requires that the skill author must not be the sole approver of their own change PR.
- **Recommendation:** Add to Registry: `Reviewer: Peer review required before merging changes to main — skill author must not be sole approver.` Consider a brief note that changes must be raised as PRs and reviewed by a second team member or tech lead before merge.

## Medium-Criticality Failures

### **Subcategory:** Versioning strategy defined
- **Category:** Maintainability & Lifecycle (Cat 11)
- **Finding:** Registry states `Version: 1.0.0` but no versioning strategy is defined — no notes on how versions are bumped, no rollback plan, no statement of production pinning policy.
- **Recommendation:** Add versioning strategy note: e.g. "Pinned to `main`; changes via PR with peer review; rollback by reverting the PR. Version bumped on breaking interface changes."

### **Subcategory:** Package install commands explicit; no assumed installations
- **Category:** Anti-Pattern Audit (Cat 13)
- **Finding:** Prerequisites references `pip install -r requirements.txt` but no `requirements.txt` file exists in the skill folder. The Registry mentions `requests==2.33.1` as a dependency but there is no machine-readable lockfile.
- **Recommendation:** Create `requirements.txt` in the skill root containing `requests==2.33.1`. This is a low-effort fix that makes the install command work verbatim.

### **Subcategory:** `reference.md` Authentication section is stale
- **Category:** Instructions Clarity (Cat 3) — note only; category remains PASS (carried)
- **Finding:** `reference.md` Authentication section still describes the old `POST /api/v1/users/login` flow with Base64-encoded password. This was the auth mechanism used by the previous `publish_vocab.py`. The script now uses a Bearer JWT from `OM_TOKEN` directly — the login endpoint is no longer called. This creates documentation drift between `reference.md` and both the current script and SKILL.md.
- **Recommendation:** Update the Authentication section of `reference.md` to describe the current approach: "Authentication uses a long-lived OpenMetadata Bot JWT token supplied via `OM_TOKEN`. The script sets `Authorization: Bearer <token>` on all requests. No login endpoint is called." Remove or move the old `POST /api/v1/users/login` description to an "Old patterns" subsection if it is needed for historical reference.

## Low-Criticality Failures

### **Subcategory:** Coexistence behavior documented
- **Category:** Coexistence & Recall (Cat 8)
- **Finding:** No documentation of how this skill coexists with `catalog-sync` (which also interacts with OpenMetadata) or `governance-vocab-generator` (adjacent trigger space). Potential for routing confusion on "sync governance tags" prompts.
- **Recommendation:** Add a brief coexistence note: "This skill publishes vocabulary only. For catalog ingestion use `catalog-sync`; for vocabulary generation use `governance-vocab-generator`."

## Strengths

- **Auth mismatch resolved:** `publish_vocab.py` now uses `OM_BASE_URL` and `OM_TOKEN` environment variables exclusively, matching the SKILL.md workflow exactly. The script docstring documents this clearly. No CLI credential flags, no login endpoint, no Base64 password encoding — all previously identified Cat 7 concerns are resolved.
- **Excellent error-handling documentation:** 401 and 403 error branches are clearly described with exact remediation steps including UI navigation paths. Most skills omit this level of detail.
- **Strong gotchas section:** Five well-chosen operational lessons covering bot policy requirements, HTTP/HTTPS gotcha, PowerShell quirks, token expiry, and `.env` git-ignore — all from real failure patterns.
- **Credential security section:** Explicit guidance on never logging tokens, CI/CD injection patterns, and `.env` handling — above average for a skill of this maturity level.
