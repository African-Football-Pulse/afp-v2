import json
import argparse
from datetime import datetime
from src.gpt import render_gpt   # <-- ändrad import
import logging

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a duo dialogue lasting ~60–70 seconds (≈160–200 words).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona blocks.
- Fold in news facts without inventing specifics.
- Alternate lines naturally between the two personas.
- Avoid placeholders like [TEAM]; use only info present in the news input.
- Keep it record-ready: natural back-and-forth, 6–8 short exchanges.
- End with a joint crisp takeaway line.
"""

def build_section(args):
    logging.info("[s_opinion_duo_experts] START")

    with open(args.news[0], "r", encoding="utf-8") as f:
        news_items = json.load(f)

    # Personas
    with open(args.personas, "r", encoding="utf-8") as f:
        personas = json.load(f)
    ids = args.persona_ids.split(",")
    p1, p2 = personas[ids[0]], personas[ids[1]]

    prompt = f"""Personas: {json.dumps([p1, p2])}
News items: {json.dumps(news_items[:3])}

Write a ~60–70s expert dialogue as if spoken by these two personas.
"""

    output = render_gpt(SYSTEM_RULES, prompt)

    section = {
        "section": "OPINION.DUO_EXPERTS",
        "date": args.date,
        "personas": ids,
        "script": output,
        "sources": [n.get("link") for n in news_items[:3]]
    }

    logging.info("[s_opinion_duo_experts] DONE")
    return section
