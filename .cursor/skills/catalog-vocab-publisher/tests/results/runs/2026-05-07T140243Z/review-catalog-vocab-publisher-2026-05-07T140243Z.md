# Skill Review: catalog-vocab-publisher

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

The skill has strong documentation, clear error-handling workflows, good trigger coverage, and well-written gotchas. However, it contains a critical script/SKILL.md auth mismatch: `publish_vocab.py` still requires `--username` and `--password` CLI arguments and authenticates via `POST /api/v1/users/login`, while the updated SKILL.md instructs users to set `OM_TOKEN` (JWT) with no password. The run command shown in the SKILL.md workflow is also incomplete — it omits the required `--base-url`, `--username`, and `--password` args, meaning it would fail immediately if executed verbatim. Additionally, no separation-of-duties requirement is documented for skill changes, and no versioning strategy is defined. These issues must be resolved before production deployment.

## Unit Tests: 10 / 13 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Basic publish trigger | Should-trigger | ✅ PASS | |
| 2 | Named file push with JWT auth | Should-trigger | ❌ FAIL | Agent correctly in Step 1; literal assertion "includes publish_vocab.py command" not met |
| 3 | Idempotent re-run | Should-trigger | ❌ FAIL | Agent correctly in Step 1; "includes publish_vocab.py command" assertion not met |
| 4 | 401 error handling | Should-trigger | ✅ PASS | |
| 5 | 403 error handling | Should-trigger | ✅ PASS | |
| 6 | Upload framework | Should-trigger | ✅ PASS | |
| 7 | Push updated descriptions | Should-trigger | ❌ FAIL | Neither UPDATED vs SKIPPED explanation nor command shown |
| 8 | Exact trigger phrase | Should-trigger | ✅ PASS | |
| 9 | Should-not-trigger: vocab generation | Should-not-trigger | ✅ PASS | |
| 10 | Should-not-trigger: Snowflake ingestion | Should-not-trigger | ✅ PASS | |
| 11 | Edge: no file path | Edge case | ✅ PASS | |
| 12 | Edge: service account not set up | Edge case | ✅ PASS | |
| 13 | Edge: SSL errors | Edge case | ✅ PASS | |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS | Name valid (23 chars), what/when/triggers all present, 5 explicit trigger phrases |
| 2 | Anatomy & Structure | PASS | Valid frontmatter, ~139 lines (well under 500), references shallow (2 hops max) |
| 3 | Instructions Clarity | PASS | Step-by-step workflow, explicit 401/403 error branches, consistent terminology |
| 4 | Output Quality | PASS | Example output block defines exact CREATED/UPDATED/SKIPPED format |
| 5 | Testability | PASS | Tests created this run; all HIGH subcategories pass |
| 6 | Resource Efficiency | PASS | Pre-built script invoked deterministically; no over-explanation of common knowledge |
| 7 | Security & Trust | FAIL | [HIGH] Script/SKILL.md auth mismatch — see High Failures |
| 8 | Coexistence & Recall | PASS | Specific triggers; coexistence testing absent (LOW only) |
| 9 | Model Compatibility | PASS | `validated_on: Claude Sonnet 4.6` in frontmatter and Registry |
| 10 | Workflow & Feedback Loops | PASS | Confirm → run → review loop; 401/403 fix-and-retry paths present |
| 11 | Maintainability & Lifecycle | FAIL | [HIGH] No separation-of-duties documented; [MEDIUM] versioning strategy missing |
| 12 | Gotchas / Lessons Learned | PASS | 5 real-world gotchas documented (bot policy, HTTP/HTTPS, PowerShell, token expiry, .env) |
| 13 | Anti-Pattern Audit | PASS | No voodoo constants; explicit error handling; requirements.txt referenced but missing (MEDIUM, 3/4 pass) |

## High-Criticality Failures

### **Subcategory:** Bundled scripts reviewed and behavior matches stated purpose
- **Category:** Security & Trust (Cat 7)
- **Finding:** `publish_vocab.py` authenticates via `POST /api/v1/users/login` with `--username` (email) and `--password` (required CLI args), Base64-encoding the password in-flight. The updated SKILL.md specifies `OM_TOKEN` JWT authentication and no password. The SKILL.md workflow command (`python publish_vocab.py --file <path>`) omits all three required args (`--base-url`, `--username`, `--password`) — running it verbatim would immediately error with `argument --base-url is required`. Passwords passed as CLI args also appear in `ps aux` and shell history.
- **Recommendation:** Update `publish_vocab.py` to accept `OM_TOKEN` (from env var) and `OM_BASE_URL` (from env var) instead of `--username`/`--password`. Remove the `login()` function. Replace with `headers = {"Authorization": f"Bearer {os.environ['OM_TOKEN']}"}`. Remove `--base-url`, `--username`, `--password` CLI args; the script becomes `python publish_vocab.py --file <path>` exactly as the SKILL.md instructs.

### **Subcategory:** Separation of duties observed
- **Category:** Maintainability & Lifecycle (Cat 11)
- **Finding:** The Registry table lists "Data Platform team" as owner but contains no requirement for peer review before merging changes to this skill. No PR review process or approval gate is documented.
- **Recommendation:** Add to Registry: `Reviewer: Peer review required before merging changes to main`. Consider noting that the skill author must not be the sole approver of their own change PR.

## Medium-Criticality Failures

### **Subcategory:** Versioning strategy defined
- **Category:** Maintainability & Lifecycle (Cat 11)
- **Finding:** Registry states `Version: 1.0.0` but no versioning strategy is defined — no notes on how versions are bumped, no rollback plan, no statement of production pinning policy.
- **Recommendation:** Add versioning strategy note: e.g. "Pinned to `main`; changes via PR with peer review; rollback by reverting the PR."

### **Subcategory:** Package install commands explicit; no assumed installations
- **Category:** Anti-Pattern Audit (Cat 13)
- **Finding:** Prerequisites says `pip install -r requirements.txt` but no `requirements.txt` file exists in the skill folder. The Registry mentions `requests==2.33.1` as a dependency but there is no machine-readable lockfile.
- **Recommendation:** Create `requirements.txt` in the skill root containing `requests==2.33.1`. Update the `reference.md` auth section once the script is updated to remove the password-based login.

## Low-Criticality Failures

### **Subcategory:** Coexistence behavior documented
- **Category:** Coexistence & Recall (Cat 8)
- **Finding:** No documentation of how this skill coexists with `catalog-sync` (which also interacts with OpenMetadata) or `governance-vocab-generator` (adjacent trigger space). Potential for routing confusion on "sync governance tags" prompts.
- **Recommendation:** Add a brief coexistence note: "This skill publishes vocabulary only. For catalog ingestion use `catalog-sync`; for vocabulary generation use `governance-vocab-generator`."

## Strengths

- **Excellent error-handling documentation:** 401 and 403 error branches are clearly described with exact remediation steps including UI navigation paths. Most skills omit this level of detail.
- **Strong gotchas section:** Five well-chosen operational lessons covering bot policy requirements, HTTP/HTTPS gotcha, PowerShell quirks, token expiry, and `.env` git-ignore — all from real failure patterns.
- **Idempotency highlighted:** The idempotent nature of the script is clearly documented both in the workflow and the "What the script does" section, reducing fear of re-runs.
- **Credential security section:** Explicit guidance on never logging tokens, CI/CD injection patterns, and `.env` handling — above average for a skill of this maturity level.
