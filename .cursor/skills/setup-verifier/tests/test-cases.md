# Eval Suite: setup-verifier

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "check my setup" | Loads skill; shows a table or structured list covering all four systems and .env; explains mode constraint if in Ask mode |
| 2 | "verify my connections are working" | Loads skill; shows all four systems with status indicators (✅/❌/—); does not give installation guidance |
| 3 | "validate my environment before I start work" | Loads skill; names all four systems; surfaces at least one specific fix; describes the expected output structure |
| 4 | "is my MCP access set up correctly?" | Loads skill; tests each named MCP server; reports outcomes in the defined table format |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "extract the last dbt Cloud run logs" | Triggers `dbt-cloud-log-extractor` skill, not this one |
| 2 | "run the source system analyser against my database" | Triggers `source-system-analyser` skill, not this one |
| 3 | "how do I install Cursor?" | General knowledge / onboarding doc; no skill triggered |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "just check my Snowflake connection" | Skill runs full check (scoping not documented); still produces the full table covering all four systems |
| 2 | "my setup is done" | Ambiguous — skill should not trigger on a statement; no MCP checks fired unless user follows up with a verification request |
