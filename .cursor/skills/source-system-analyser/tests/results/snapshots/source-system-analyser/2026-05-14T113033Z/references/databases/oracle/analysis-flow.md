# Oracle Analysis Flow

1. Resolve connection and Oracle driver compatibility.
2. Check for `db-analysis-config.json` in the working directory.
3. If the file is missing, ask whether to exclude schemas, exclude tables, or set `max_row_limit`; write the JSON only if the user requests at least one setting.
4. Run analyzer with `--dialect oracle`.
5. Validate entity counts and key metadata extraction.
6. Review data quality findings with emphasis on constraints and timestamp behavior.
