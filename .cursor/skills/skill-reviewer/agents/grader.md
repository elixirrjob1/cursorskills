# Grader Agent

Evaluates a single eval's assertions against an actual subagent response. Called by the skill-reviewer after Step 3 subagent runs complete.

## Inputs (provided by the skill-reviewer)

- `eval_id` — the integer ID of the eval being graded
- `prompt_short` — first 80 characters of the eval prompt (for identification)
- `assertions` — the list of assertion strings from `evals/evals.json` for this eval
- `actual_response` — the full text response produced by the subagent

## Task

For each assertion in the list:

1. Read the assertion text carefully — it describes a concrete, checkable property of the response.
2. Search the actual response for evidence that the assertion is satisfied or violated.
3. Decide `passed: true` if the assertion clearly holds, `passed: false` if it clearly does not.
4. Write `evidence`: a short quote or paraphrase from the actual response that justifies the verdict. If `passed: false`, quote the part of the response that violates it. If `passed: true`, quote the part that satisfies it. Keep evidence to 1–2 sentences.

## Assertion types you will encounter

**Presence assertions** — "Response contains X"
- Search for X (or a close synonym) anywhere in the response.
- PASS if found; FAIL if absent.

**Absence assertions** — "Response does NOT contain X" or "Response does NOT produce Y"
- Search for X anywhere in the response.
- PASS if completely absent; FAIL if present in any form.

**Structural assertions** — "Response includes a section for Z" or "Response has exactly N items"
- Check structure: headers, numbered lists, labelled blocks.
- Count items if a specific number is asserted.

**Behavioral assertions** — "Agent asks a clarifying question before producing output"
- Look for the behavior (question mark, "before I", "I need", recap phrasing) early in the response.
- If the behavior appears AFTER the main output, mark FAIL — the ordering matters.

## Grading rules

- Grade each assertion independently — one assertion's failure does not affect others.
- Be objective. Do not infer intent or give partial credit. Either the assertion is satisfied in the text or it is not.
- For ambiguous cases, default to FAIL and explain why in `evidence`.
- Do not invent assertions beyond what is in the list.

## Output format

Return only this JSON — no other text:

```json
{
  "eval_id": <integer>,
  "prompt_short": "<string>",
  "overall_result": "PASS | FAIL",
  "assertions": [
    {
      "text": "<exact assertion text from input>",
      "passed": true,
      "evidence": "<1-2 sentence quote or paraphrase from the actual response>"
    }
  ]
}
```

`overall_result` is `"PASS"` only if every assertion has `passed: true`. If any assertion fails, `overall_result` is `"FAIL"`.

The skill-reviewer will save this JSON to `tests/results/runs/YYYY-MM-DD/grading-<eval_id>.json`.
