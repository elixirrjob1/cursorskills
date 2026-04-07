# GlossarySkill — Setup & Usage Guide

A Cursor-based agent skill that generates domain business glossaries and pushes them into OpenMetadata via MCP.

---

## Prerequisites

- [Cursor IDE](https://cursor.sh/) installed
- Python 3.10+ available on your PATH
- Access to an OpenMetadata instance (URL, email, and password)

---

## 1. Set Up the Virtual Environment

Ask the agent to create and configure the virtual environment for you:

> *"Create a virtual environment for this project and install all dependencies inside it."*

The agent will run the following steps on your behalf:

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1   # Windows
pip install -r requirements.txt
```

> Do not install Python packages manually or globally — always let the agent install everything inside the venv.

---

## 2. Create the `.env` File

Create a `.env` file in the project root and add:

```env
KEYVAULT_NAME=skills-fmaric-kv
FIVETRAN_API_KEY=
FIVETRAN_API_SECRET=
TARGET=
SNOWFLAKE_PAT=
SNOWFLAKE_WAREHOUSE=FIVETRAN_DRIP_WH
SNOWFLAKE_ACCOUNT=ZNA09333-IOLAP_PARTNER
SNOWFLAKE_FIVETRAN_PASSWORD=
SNOWFLAKE_SQL_API_HOST=https://ZNA09333-IOLAP_PARTNER.snowflakecomputing.com
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_ENDPOINT=https://fmarichub25942327190.cognitiveservices.azure.com/
AZURE_OPENAI_DEPLOYMENT=gpt-4o
OPENMETADATA_BASE_URL=http://52.255.209.74:8585
OPENMETADATA_EMAIL=
OPENMETADATA_PASSWORD=
SNOWFLAKE_DATABASE=DRIP_DATA_INTELLIGENCE
```

Replace the placeholders with the values from your OpenMetadata instance metadata (email and password).

---

## 3. Add MCP Server Path to Cursor

Ask the agent to register the OpenMetadata MCP server in your Cursor config:

> *"Add the MCP server path to `~/.cursor/mcp.json`."*

The agent will add the correct entry to your Cursor global MCP config at `~/.cursor/mcp.json`:

Restart Cursor after the agent completes this step.

---

## 4. Troubleshooting

### ModuleNotFoundError: No module named `pydantic_core._pydantic_core`

This error means the virtual environment is not active or dependencies were installed outside of it. 

### Agent model errors / slow responses

If you encounter errors with the default model:
1. In Cursor, turn off **Auto** model selection.
2. Manually select **claude-opus-4-6**.
3. Paste the error message directly into the chat — the agent will diagnose and fix it.

---

## 5. Verify MCP Is Working

Ask the agent to fetch the current glossary to confirm the MCP server is connected:

> *"Fetch the list of glossaries from OpenMetadata."*

A successful response will return the glossary list from your OpenMetadata instance.

---

## 6. Generate a Business Glossary

Use the **schema-glossary-generator** skill to create a glossary from a plain-language business description.

### Example prompt

```
Use the schema-glossary-generator skill and generate a glossary with the following description:

We operate a lending and credit management platform within a retail and commercial banking institution.
The system manages the full credit lifecycle — from customer origination and credit application through
underwriting, disbursement, servicing, collections, and portfolio reporting.
```

The agent will produce:

- `<stem>_glossary.json` — structured glossary following the output contract
- `<stem>_glossary.xlsx` — formatted Excel workbook with dropdown status validation

---

## 7. Push the Glossary to OpenMetadata

Once the glossary JSON is generated, ask the agent to push it:

> *"Use the MCP server to create this glossary and push it into OpenMetadata."*

The agent will:
1. Create the glossary entity in OpenMetadata.
2. Create each glossary term with its definition, synonyms, business usage, and term type.

---

## Project Structure

```
GlossarySkill/
├── .env                          # Credentials (not committed)
├── .venv/                        # Python virtual environment
├── requirements.txt              # Python dependencies
├── README.md                     # This file
├── lending_credit_platform.txt   # Example business brief
├── lending_credit_platform_glossary.json   # Generated glossary JSON
├── lending_credit_platform_glossary.xlsx   # Generated Excel export
└── cursorskills/
    └── .cursor/
        └── skills/
            └── schema-glossary-generator/
                ├── SKILL.md
                ├── references/
                │   └── output-contract.md
                └── scripts/
                    ├── generate_glossary.py
                    └── glossary_json_to_excel.py
```

---

## Glossary Term Types

| Type | Description |
|------|-------------|
| `business_entity` | Core domain objects (customer, facility, collateral) |
| `business_process` | Workflows and lifecycle stages |
| `business_measure` | Quantitative metrics |
| `business_attribute` | Descriptive properties of entities |
| `status` | Workflow or classification states |
| `identifier` | System or business keys |
| `relationship` | Links between entities |
| `inferred_concept` | Terms implied by business context, not explicit in the brief |

---

## Notes

- All generated terms carry `status: draft` by default — review and promote them in OpenMetadata as appropriate.
- Never commit the `.env` file to version control.
