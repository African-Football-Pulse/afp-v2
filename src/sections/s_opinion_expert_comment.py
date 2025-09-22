import os
import json
from src.gpt import run_gpt

def build_section(args):
    # Normalisera path till projektroten
    news_path = os.path.join(os.getcwd(), args.news[0])
    if not os.path.exists(news_path):
        raise FileNotFoundError(f"Could not find candidates file: {news_path}")

    with open(news_path, "r", encoding="utf-8") as f:
        candidates = [json.loads(line) for line in f]

    # Bygg GPT-prompt
    SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a single speaker monologue that lasts ~45 seconds (≈120–160 words).
Constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona.
- Use only facts from the input, no invention.
- Flowing spoken style, not list format.
- End with a crisp takeaway line.
"""

    persona = "AK"  # hårdkodat nu – styrs via personas.json senare
    prompt = {
        "system": SYSTEM_RULES,
        "persona": persona,
        "news": candidates
    }

    text = run_gpt(prompt)

    return {
        "section": args.section,
        "items": [{"persona": persona, "text": text}]
    }
