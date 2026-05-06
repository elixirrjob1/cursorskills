# Eval Suite: natural-language-data-query

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "What were total sales last quarter?" | Agent searches OM for sales/fact tables, generates Snowflake SQL with DATE_TRUNC quarter pattern anchored to MAX date, executes, presents result |
| 2 | "How many customers placed orders in the last 30 days?" | Agent discovers customer/orders tables via OM keyword search, generates COUNT with DATEADD filter, executes, presents number |
| 3 | "Show me revenue by product category for this year" | Agent searches OM for revenue + product tables, generates GROUP BY SQL using Gold/Curated layer if available, executes, presents table |
| 4 | "Can you look up inventory levels for the warehouse?" | Agent triggers on "can you look up" + "inventory", searches OM, generates SQL, executes |
| 5 | "Give me a report on top 10 suppliers by purchase volume" | Agent triggers on "report", searches OM for supplier/purchase tables, generates ORDER BY + LIMIT 10 SQL, executes |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Generate a dbt model for the sales table" | Should be handled by dbt-model-from-stm skill — not a data question, no SQL execution needed |
| 2 | "Write me a Python script to connect to Snowflake" | General coding task — agent answers from general knowledge, does not invoke this skill |
| 3 | "Can you create a new glossary term in OpenMetadata?" | Should be handled by catalog-sync or catalog-vocab-publisher skill — write operation, not a data query |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Can you look something up?" | Skill fires (matches "can you look up") but Step 1 says to collect the business question — agent should ask: "What would you like me to look up? Please give me a business question." |
| 2 | "Show me everything" | Skill fires on "show me data about" proximity but query is too broad — agent should ask for clarification on domain/subject before proceeding |
| 3 | "What's in the data?" | Vague — skill may fire on "data question" trigger; agent should ask for a specific business question rather than attempting a broad metadata scan |
