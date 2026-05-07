# Eval Suite: meetingintroskill

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "I'm walking into a discovery call with the VP of Engineering at Acme Corp (SaaS) tomorrow — keep it warm and brief" | Tests clear-input fast path: all three dimensions (audience, style, industry) are inferable. Agent must NOT ask clarifying questions. Must produce exactly 5 intros using all 5 strategies. Each intro must begin with a greeting prefix. Must recommend one winner with reasoning. |
| 2 | "Need an opener for a meeting" | Tests vague-input clarification path: audience, style, and industry are all missing. Agent must ask minimal clarifying questions covering the missing dimensions before drafting any intros. Must NOT produce the five intros before clarification is confirmed. |
| 3 | "Meeting tomorrow with GreenPath Solutions — first call" | Tests ambiguous company name → industry rule: "GreenPath Solutions" is not a well-known brand, industry cannot be safely inferred. Agent must ask for industry or field explicitly before proceeding. Must not guess and produce intros. |
| 4 | "I have a kickoff with the Head of Procurement and two category managers at a mid-size manufacturing firm — I want to sound competent without being stiff" | Tests full-input path with consultative style: all dimensions inferrable. Agent must produce exactly 5 intros, one per strategy, each with greeting prefix. Winner recommendation must explain why it fits a procurement/manufacturing audience. |
| 5 | "What should I say right after my intro lands — I'm meeting the CHRO of a UK fintech startup, formal tone" | Tests "what to say right after" optional output: skill explicitly covers surfacing a handoff line. Agent must produce 5 intros AND include the optional exit/handoff section at the end of the recommendation. |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Draft a follow-up email to my client after yesterday's call — they seemed interested in the pricing tier" | Should NOT trigger — this is post-meeting written communication, not a first spoken meeting opener. Agent should handle it using general writing/email capability. |
| 2 | "Help me build a sales pitch deck for our new product launch" | Should NOT trigger — this is slide/presentation content, not a spoken first-meeting opener. Agent should use general document or presentation skill. |
| 3 | "How should I answer 'tell me about yourself' in my job interview at Deloitte?" | Should NOT trigger — this is interview prep for a candidate, not a first commercial client meeting opener. Agent handles it directly without this skill. |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Write me a meeting intro for JPMorgan Chase — first time meeting their risk team" | Tests high-stakes regulated inference: JPMorgan Chase is clearly finance/banking. Agent must infer finance confidently (well-known brand) and produce 5 intros — it must NOT ask to confirm the industry. |
| 2 | "Give me an opener — we've been working with this client for two years and this is our quarterly review" | Tests scope boundary: the skill is scoped to first meetings where the relationship is being established. Agent should either ask a clarifying question about whether this really is a first meeting context, or flag that the skill is designed for first meetings and ask how to proceed. Must NOT silently produce intros as if it were a first-meeting scenario. |
| 3 | "I need a meeting intro for my internal all-hands where I'm introducing a new initiative to my own team" | Tests non-client boundary: this is an internal meeting, not a first client commercial conversation. Agent should clarify this skill covers first external client meetings, and ask whether they want to proceed in that spirit or need something different. |
