import os
import json
from src.gpt import run_gpt

def build_section(args):
    news_path = os.path.join(os.getcwd(), args.news[0])
    if not os.path.exists(news_path):
        raise FileNotFoundError(f"Could not find candidates file: {news_path}")

    with open(news_path, "r", encoding="utf-8") as f:
        candidates = [json.loads(line) for line in f]

    SYSTEM_RULES = """You are writing a dialogue for a football podcast.
Two personas are debating (~60–80 words each).
Constraints:
- Output MUST be in English.
- Stay in character for both personas.
- Use only facts from input, no invention.
- Conversational flow with light tension.
- Conclude with a short shared reflection.
"""

    personas = ["AK", "JJK"]  # hårdkodat nu – styrs via personas.json senare
    prompt = {
        "system": SYSTEM_RULES,
        "personas": personas,
        "news": candidates
    }

    text = run_gpt(prompt)

    return {
        "section": args.section,
        "items": [{"personas": personas, "text": text}]
    }
