# Flat Format Detection

Determine source format before inference:

- `.csv` / `.tsv`: delimited text
- `.json` / `.ndjson`: object or event records
- `.xlsx`: worksheet-oriented tabular data

Capture delimiter, quote rules, encoding, header presence, and sheet name (if relevant) in metadata findings.

## CSV Delimiter Rules

- Default behavior: auto-detect delimiter for CSV from sample content.
- Supported sniffed delimiters: `,`, `;`, `\\t`, `|`.
- If auto-detection is wrong or source is inconsistent, provide delimiter explicitly:
  - `--columns-delimiter "<char>"`
  - `--tables-delimiter "<char>"`
