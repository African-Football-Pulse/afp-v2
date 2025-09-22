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
        print(f"[opinion_expert] WARN: candidates file missing → {news_path}")
        return {
            "section": "S.OPINION.EXPERT_COMMENT",
            "content": "No candidates available."
        }

    # Läs in kandidater
    with open(news_path, "r", encoding="utf-8") as f:
        candidates = [json.loads(line) for line in f]

    if not candidates:
        print("[opinion_expert] WARN: No candidates found inside file")
        return {
            "section": "S.OPINION.EXPERT_COMMENT",
            "content": "No candidates available."
        }

    # Använd top candidate
    top_item = candidates[0]
    news_text = top_item.get("text", "")

    # GPT prompt
    prompt = f"""You are an expert commentator for African football.
Write a short (~120 words) spoken-style comment about this news:

{news_text}

Stay natural, insightful, and record-ready."""
    gpt_output = run_gpt(prompt)

    return {
        "section": "S.OPINION.EXPERT_COMMENT",
        "content": gpt_output,
        "source_news": news_text,
    }
