# src/sections/s_opinion_duo_experts.py
import os
import json
from src.gpt import run_gpt
from src.sections.utils import write_outputs


def build_section(args):
    """Produce a duo-expert opinion section with GPT commentary."""
    raw_path = args.news[0] if args.news else None
    if not raw_path:
        raw_path = f"producer/candidates/{args.date}/scored.jsonl"
    news_path = os.path.abspath(raw_path)

    if not os.path.exists(news_path):
        print(f"[opinion_duo] WARN: candidates file missing → {news_path}")
        payload = {
            "slug": "opinion_duo_experts",
            "title": "Duo Expert Opinion",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {"news_input_path": news_path},
            "meta": {"persona": "Expert Duo"},
            "type": "opinion",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(args.section, args.date, args.league or "_", payload, status="no_candidates")

    with open(news_path, "r", encoding="utf-8") as f:
        candidates = [json.loads(line) for line in f]

    if not candidates:
        print("[opinion_duo] WARN: No candidates found inside file")
        payload = {
            "slug": "opinion_duo_experts",
            "title": "Duo Expert Opinion",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {"news_input_path": news_path},
            "meta": {"persona": "Expert Duo"},
            "type": "opinion",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(args.section, args.date, args.league or "_", payload, status="no_candidates")

    # Använd de två främsta kandidaterna
    first_item = candidates[0]
    second_item = candidates[1] if len(candidates) > 1 else candidates[0]
    news_text = f"- {first_item.get('text','')}\n- {second_item.get('text','')}"

    # GPT prompt för dialog
    prompt = f"""You are two African football experts debating recent news.
Have a lively, 2-voice exchange (~140 words total) based on these stories:

{news_text}

Make it conversational, record-ready, and avoid lists or placeholders."""
    gpt_output = run_gpt(prompt)

    payload = {
        "slug": "opinion_duo_experts",
        "title": "Duo Expert Opinion",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"news_input_path": news_path},
        "meta": {"persona": "Expert Duo"},
        "type": "opinion",
        "model": "gpt-4o-mini",
        "items": [first_item, second_item],
    }

    return write_outputs(args.section, args.date, args.league or "_", payload, status="ok")
