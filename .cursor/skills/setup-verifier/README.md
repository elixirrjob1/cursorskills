# setup-verifier

A Cursor Agent skill that verifies a new team member's local setup in one go.

## What it does

Ask Cursor: **"Check my setup"**

The skill runs live checks against all four MCP servers and validates required `.env` variables, then returns a pass/fail table with a specific fix for every failure.

| Check | What is tested |
|---|---|
| Snowflake | Connects and returns current user, role, and warehouse |
| OpenMetadata | Lists database services (at least one expected) |
| dbt Cloud | Lists jobs (at least one expected) |
| Fivetran | Lists connectors (at least one expected) |
| `.env` variables | All required keys are present and non-empty |

## Usage

Open any repo in Cursor that has this skill installed (via `responsum-team/plugins`) and say:

> *"Check my setup"*
> *"Verify my connections are working"*
> *"Validate my environment before I start work"*

The skill requires **Agent mode** — live MCP calls do not run in Ask/read-only mode.

## Requirements

- Cursor with Agent mode enabled
- All four MCP servers configured in `mcp.json` (`project-0-cursorskills-snowflake`, `user-openmetadata`, `project-0-cursorskills-dbt`, `user-fivetran-example`)
- A populated `.env` (see [ONBOARDING.md](https://github.com/responsum-team/dbtproject/blob/main/ONBOARDING.md) for the full variable list)

## Files

| File | Purpose |
|---|---|
| `SKILL.md` | Skill definition read by the Cursor agent |
| `tests/test-cases.md` | Human-readable behavioral assertions |
| `tests/evals/evals.json` | Machine-readable eval assertions |
| `tests/test_skill.py` | pytest unit tests for skill structure |
| `tests/results/` | Archived review runs from `skill-reviewer` |
