# tests/test_skill.py — meetingintroskill
# Run with: pytest tests/test_skill.py
#
# These are structural assertion helpers. Actual subagent responses are
# evaluated by the skill-reviewer agent against test-cases.md expected
# behaviors. Use these functions to programmatically check response text
# when responses are captured.

import re


def has_five_strategies(text: str) -> bool:
    """Returns True if the response contains references to all 5 strategy names."""
    strategies = ["relationship", "purpose", "curiosity", "proof", "time"]
    text_lower = text.lower()
    return all(s in text_lower for s in strategies)


def has_greeting_prefix(intro_text: str) -> bool:
    """Returns True if the intro contains a greeting element (name/hello/good to meet)."""
    greeting_signals = ["good morning", "good afternoon", "hi ", "hello", "great to meet",
                        "nice to meet", "glad to", "thanks for", "thank you for"]
    text_lower = intro_text.lower()
    return any(s in text_lower for s in greeting_signals)


def has_winner_recommendation(text: str) -> bool:
    """Returns True if the response names a recommended strategy."""
    signals = ["recommend", "best fit", "winner", "i'd go with", "i would go with",
               "best option", "strongest choice"]
    text_lower = text.lower()
    return any(s in text_lower for s in signals)


def has_clarifying_question(text: str) -> bool:
    """Returns True if the response contains a clarifying question."""
    return "?" in text


def has_handoff_line(text: str) -> bool:
    """Returns True if the response includes guidance on what to say after the intro."""
    signals = ["after", "follow", "handoff", "transition", "next", "from there",
               "once you've", "that opens", "then you can"]
    text_lower = text.lower()
    return any(s in text_lower for s in signals)


# --- Should-trigger tests ---

def test_should_trigger_clear_input_produces_five_intros():
    """I'm walking into a discovery call with VP Engineering at Acme Corp (SaaS) — warm and brief."""
    # Assert: no clarifying questions; exactly 5 strategy intros; winner named
    # Evaluated by subagent response + has_five_strategies() + has_winner_recommendation()
    pass


def test_should_trigger_vague_input_asks_before_drafting():
    """Need an opener for a meeting."""
    # Assert: response contains clarifying question; does NOT contain 5 intros
    # Evaluated by subagent response + has_clarifying_question(); NOT has_five_strategies()
    pass


def test_should_trigger_ambiguous_company_asks_industry():
    """Meeting tomorrow with GreenPath Solutions — first call."""
    # Assert: response asks for industry; does NOT produce 5 intros
    # Evaluated by has_clarifying_question(); NOT has_five_strategies()
    pass


def test_should_trigger_manufacturing_kickoff_full_output():
    """Kickoff with Head of Procurement at mid-size manufacturing firm."""
    # Assert: 5 intros; winner mentions manufacturing/procurement
    # Evaluated by has_five_strategies() + winner reasoning check
    pass


def test_should_trigger_handoff_line_included():
    """What to say right after intro — CHRO of UK fintech, formal."""
    # Assert: 5 intros + handoff section present
    # Evaluated by has_five_strategies() + has_handoff_line()
    pass


# --- Should-not-trigger tests ---

def test_should_not_trigger_follow_up_email():
    """Draft a follow-up email to my client after yesterday's call."""
    # Assert: response addresses email; does NOT produce 5 meeting opener strategies
    # Evaluated by NOT has_five_strategies()
    pass


def test_should_not_trigger_pitch_deck():
    """Help me build a sales pitch deck for our new product launch."""
    # Assert: addresses presentation/deck; does NOT trigger meeting opener skill
    pass


def test_should_not_trigger_interview_prep():
    """How should I answer 'tell me about yourself' in my job interview at Deloitte?"""
    # Assert: addresses interview self-intro; does NOT produce client meeting openers
    pass


# --- Edge case tests ---

def test_edge_case_well_known_bank_no_industry_confirm():
    """Give me an opener for my first meeting with JPMorgan Chase's risk team."""
    # Assert: does NOT ask to confirm industry; produces 5 intros with finance context
    # Evaluated by NOT has_clarifying_question() about industry + has_five_strategies()
    pass


def test_edge_case_ongoing_relationship_flags_scope():
    """Opener for quarterly review — we've worked with this client two years."""
    # Assert: flags scope mismatch or asks clarifying question; does NOT silently produce first-meeting intros
    # Evaluated by has_clarifying_question() or scope-flag signal in response
    pass


def test_edge_case_internal_meeting_flags_scope():
    """Meeting intro for internal all-hands introducing a new initiative to my own team."""
    # Assert: flags that skill targets first external client meetings; asks how to proceed
    # Evaluated by presence of scope-flag signal in response
    pass
