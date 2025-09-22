import os
import json
from src.gpt import run_gpt

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a short back-and-forth dialogue (~60–90 seconds, ≈160–220 words).
Hard constraints:
- Output MUST be in English.
- Use the provided persona blocks faithfully for tone & style.
- Base everything on the news input, no inventions.
- Write in a conversational spoken style, 4–6 turns max.
- Avoid list formats, keep it natural and fluid.
- End with a memorable closing line.
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
        return {"section": "S.OPINION.DUO_EXPERTS", "content": "No candidates available."}

    top_item = candidates[0]

    persona_ids = getattr(args, "persona_ids", "AK,JJK").split(",")
    with open("config/personas.json", "r", encoding="utf-8") as pf:
        personas = json.load(pf)

    persona_blocks = [personas.get(pid, {}) for pid in persona_ids]

    prompt = f"""
Personas:
{json.dumps(persona_blocks, indent=2)}

News facts:
{json.dumps(top_item, indent=2)}

Write a duo expert dialogue (~160–220 words, 4–6 exchanges).
"""

    script = run_gpt(SYSTEM_RULES, prompt)

    return {
        "section": "S.OPINION.DUO_EXPERTS",
        "persona_ids": persona_ids,
        "content": script.strip(),
    }
