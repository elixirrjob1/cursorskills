---
name: meetingintroskill
description: >-
 Use when someone is about to walk into a first client meeting and needs an opening they can actually say out loud — not a template, not a deck intro, but a real spoken line that fits the room. Covers discovery calls, kickoffs, and any first commercial conversation where the relationship is still being established. Stops for clarification when the request is too vague; otherwise produces five distinct openers across different strategies, picks the best one with reasoning, and optionally surfaces what to say right after the intro lands.
---

# First Client Meeting Opener

Help the user open a **first meeting** with a **new client**. Output must be easy to say aloud and appropriate for minute one of a new commercial relationship.

## Inputs to lock before writing

Collect or infer these three dimensions:

1. **Audience** — Who is on the call (roles, seniority, group size if known).  
2. **Style** — Desired tone and shape (e.g. formal, warm, direct, consultative, brief).  
3. **Company** — Client organization name when provided.  
4. **Industry / field** — Sector or domain (e.g. retail, fintech, public sector, industrial B2B).

**Industry rule**

- If the user gives a **company name**, infer the most likely industry or field from public-knowledge associations with that name.  
- If the name is **missing**, **generic**, or **too ambiguous** to infer safely, **ask** for the industry or field explicitly.  
- For **high-stakes or regulated** contexts (finance, health, legal, government), if inference is uncertain, **confirm** the industry with the user rather than assuming.

## Vague or underspecified requests

If the first message does not support a **confident** read on **audience**, **style**, and **industry** (after applying the industry rule above):

- Ask **minimal** follow-up questions only for what is missing or fuzzy.  
- Offer a **short recap** of what you understood.  
- Ask the user to **confirm or correct** that recap **before** you draft the five intros.

Do not output the five intros until **audience**, **style**, and **industry** are agreed or clearly stated.

## Greeting first

For **first meetings**—especially **get-to-know** or discovery—people expect a **human front door** before anything strategic.

1. **Hello + who you are** — name, role, and (if useful) organization.  
2. **Brief rapport** — e.g. thanks for the time, good to meet you, glad we could connect.  
3. **Then** the strategic beat — purpose, timeboxing, or a focused question.

**Relationship** is the natural home for steps 1–2. **Purpose**, **Curiosity**, **Proof, light**, and **Time & flow** follow the greeting—**Curiosity does not replace** “nice to meet you.”

When generating the **five** options, **prefix each spoken intro with the same short greeting** (steps 1–2) unless **Style** or the user explicitly calls for jumping straight into business. The **differentiator** across the five is still the **strategy-specific** paragraph that comes **after** that greeting.

## Five strategies (generate all five)

Produce **exactly five** distinct openings. Each option uses a **different** strategy from this list (one intro per strategy). Each option **includes greeting first**, then the strategy line (see **Greeting first** above).

| # | Strategy | Intent |
|---|----------|--------|
| 1 | **Relationship** | Warm, human icebreaker; safe for strangers; low performative risk. |
| 2 | **Purpose** | States why you are here and what you hope to learn or decide today. |
| 3 | **Curiosity** | After the greeting, a focused question or “help me understand” frame. |
| 4 | **Proof, light** | Relevant credibility in a **light** way—no feature list; may be **more than one sentence** if it still sounds spoken, not like a bio slide. |
| 5 | **Time & flow** | Honors the clock; offers a simple structure or handoff into their priorities.  

Calibrate wording to **Audience**, **Style**, and **Industry** for every option.

**Length:** Do **not** treat “intro” as “three sentences max.” Some options can be **longer** (e.g. four to eight sentences) if **Style**, **Audience**, and **natural pacing** warrant it—especially after **Greeting first**. The real limit is **time on the mic** (see **Constraints**), not an arbitrary sentence count.

## Pick one winner

After the five intros:

- Name the **recommended** option (by strategy name).  
- Explain **why it is the best fit** for this **audience**, **style**, **industry**, and **first** client meeting (trust, clarity, and appropriateness).  
- Optionally note **one runner-up** and **when** to use it instead.

The winner must be **one of the five**; do not replace it with a new combined version unless the user asks for a merge. The recommended script should **include the greeting prefix** when **Greeting first** applies.

## Response shape

1. **Context locked** — Bullets: Audience, Style, Company (if any), Industry.  
2. **Five intros** — For each: strategy label, **full spoken text** (greeting first, then strategy beat), one-line note on fit or risk.  
3. **Recommendation** — Best strategy, full text again if helpful, paragraph of reasoning, optional runner-up.

## Constraints

- Keep each intro **sayable in one go**—typical target is **well under one minute** spoken, or whatever duration the user requests. **Longer than three sentences is fine** when it still breathes and fits **Style**; avoid padding just to lengthen.  
- Avoid slang that can read as flippant in conservative industries unless **Style** invites it.  
- No **sixth** “secret best” intro in the recommendation section.  
- Skip filler, buzzword stacks, and template phrases that sound like generic sales email.
- Optionally add one to 3 sentences on how to exit the opener naturally — a handoff line that moves the room from intro into the first real exchange.