import os
import json
from src.gpt import run_gpt

def build_section(args):
    # Bestäm sökväg för candidates
    raw_path = args.news[0] if args.news else None
    if not raw_path:
        raw_path = f"producer/candidates/{args.date}/scored.jsonl"
    news_path = os.path.abspath(raw_path)

    if not os.path.exists(news_path):
        print(f"[opinion_duo] WARN: candidates file missing → {news_path}")
        return {
            "section": "S.OPINION.DUO_EXPERTS",
            "content": "No candidates available."
        }

    # Läs in kandidater
    with open(news_path, "r", encoding="utf-8") as f:
        candidates = [json.loads(line) for line in f]

    if not candidates:
        print("[opinion_duo] WARN: No candidates found inside file")
        return {
            "section": "S.OPINION.DUO_EXPERTS",
            "content": "No candidates available."
        }

    # Använd de två främsta
    first_item = candidates[0]
    second_item = candidates[1] if len(candidates) > 1 else candidates[0]

    news_text = f"- {first_item.get('text','')}\n- {second_item.get('text','')}"

    # GPT prompt för dialog
    prompt = f"""You are two African football experts debating recent news.
Have a lively, 2-voice exchange (~140 words total) based on these stories:

{news_text}

Make it conversational, record-ready, and avoid lists or placeholders."""
    gpt_output = run_gpt(prompt)

    return {
        "section": "S.OPINION.DUO_EXPERTS",
        "content": gpt_output,
        "source_news": news_text,
    }
