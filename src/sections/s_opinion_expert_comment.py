import json
import argparse
from datetime import datetime
from src.gpt import render_gpt   # <-- ändrad import
import logging

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a single speaker monologue that lasts ~45 seconds (≈120–160 words).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona block.
- Fold in news facts without inventing specifics.
- No placeholders like [TEAM]; use only info present in the news input.
- Avoid list formats; deliver a flowing, spoken monologue.
- Keep it record-ready: natural pacing, light rhetorical devices, 1–2 short pauses (…).
- End with a crisp takeaway line.
"""

def build_section(args):
    logging.info("[s_opinion_expert_comment] START")

    with open(args.news[0], "r", encoding="utf-8") as f:
        news_items = json.load(f)

    # Persona
    with open(args.personas, "r", encoding="utf-8") as f:
        personas = json.load(f)
    persona = personas[args.persona_id]

    prompt = f"""Persona: {json.dumps(persona)}
News items: {json.dumps(news_items[:3])}

Write a ~45s expert comment as if spoken by this persona.
"""

    output = render_gpt(SYSTEM_RULES, prompt)

    section = {
        "section": "OPINION.EXPERT_COMMENT",
        "date": args.date,
        "persona": args.persona_id,
        "script": output,
        "sources": [n.get("link") for n in news_items[:3]]
    }

    logging.info("[s_opinion_expert_comment] DONE")
    return section
