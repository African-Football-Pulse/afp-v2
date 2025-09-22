import os
import json
from src.gpt import run_gpt

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
    raw_path = args.news[0] if args.news else None
    if not raw_path:
        raise FileNotFoundError("No --news file provided")

    news_path = os.path.join("/app", raw_path.lstrip("/"))
    if not os.path.exists(news_path):
        raise FileNotFoundError(f"Could not find candidates file: {news_path}")

    with open(news_path, "r", encoding="utf-8") as f:
        candidates = [json.loads(line) for line in f]

    if not candidates:
        return {"section": "S.OPINION.EXPERT_COMMENT", "content": "No candidates available."}

    # Ta topprankad kandidat
    top_item = candidates[0]

    # Hämta persona-info
    persona_id = getattr(args, "persona_id", "AK")
    with open("config/personas.json", "r", encoding="utf-8") as pf:
        personas = json.load(pf)
    persona = personas.get(persona_id, {})

    prompt = f"""
Persona:
{json.dumps(persona, indent=2)}

News facts:
{json.dumps(top_item, indent=2)}

Write an expert comment monologue (~120-160 words).
"""

    script = run_gpt(SYSTEM_RULES, prompt)

    return {
        "section": "S.OPINION.EXPERT_COMMENT",
        "persona_id": persona_id,
        "content": script.strip(),
    }
