# src/sections/s_opinion_expert_comment.py
import os
import json
from src.gpt import run_gpt
from src.sections.utils import write_outputs


def build_section(args):
    """Produce a single-expert opinion section with GPT commentary."""
    raw_path = args.news[0] if args.news else None
    if not raw_path:
        raw_path = f"producer/candidates/{args.date}/scored.jsonl"
    news_path = os.path.abspath(raw_path)

    if not os.path.exists(news_path):
        print(f"[opinion_expert] WARN: candidates file missing → {news_path}")
        payload = {
            "slug": "opinion_expert_comment",
            "title": "Expert Comment",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {"news_input_path": news_path},
            "meta": {"persona": "Expert"},
            "type": "opinion",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(args.section, args.date, args.league or "_", payload, status="no_candidates")

    with open(news_path, "r", encoding="utf-8") as f:
        candidates = [json.loads(line) for line in f]

    if not candidates:
        print("[opinion_expert] WARN: No candidates found inside file")
        payload = {
            "slug": "opinion_expert_comment",
            "title": "Expert Comment",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {"news_input_path": news_path},
            "meta": {"persona": "Expert"},
            "type": "opinion",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(args.section, args.date, args.league or "_", payload, status="no_candidates")

    # Använd top candidate
    top_item = candidates[0]
    news_text = top_item.get("text", "")

    # GPT prompt
    prompt = f"""You are an expert commentator for African football.
Write a short (~120 words) spoken-style comment about this news:

{news_text}

Stay natural, insightful, and record-ready."""
    gpt_output = run_gpt(prompt)

    payload = {
        "slug": "opinion_expert_comment",
        "title": "Expert Comment",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"news_input_path": news_path},
        "meta": {"persona": "Expert"},
        "type": "opinion",
        "model": "gpt-4o-mini",
        "items": [top_item],
    }

    return write_outputs(args.section, args.date, args.league or "_", payload, status="ok")
