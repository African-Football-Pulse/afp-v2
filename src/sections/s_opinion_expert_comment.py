import os
from src.gpt import run_gpt
from src.sections.utils import write_outputs, load_candidates

def build_section(args):
    """Produce a single-expert opinion section with GPT commentary."""
    candidates, blob_path = load_candidates(args.date, args.news[0] if args.news else None)

    if not candidates:
        print(f"[opinion_expert] WARN: No candidates available in {blob_path}")
        payload = {
            "slug": "opinion_expert_comment",
            "title": "Expert Comment",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {"news_input_path": blob_path},
            "meta": {"persona": "Expert"},
            "type": "opinion",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(args.section, args.date, args.league or "_", payload, status="no_candidates")

    # Ta top candidate
    top_item = candidates[0]
    news_text = top_item.get("text", "")

    # GPT setup
    prompt_config = {
        "persona": "Expert commentator for African football",
        "instructions": f"Write a short (~120 words) spoken-style comment about this news:\n\n{news_text}\n\nStay natural, insightful, and record-ready."
    }
    ctx = {"candidates": [top_item]}
    system_rules = "You are an assistant generating natural spoken-style football commentary."

    gpt_output = run_gpt(prompt_config, ctx, system_rules)

    payload = {
        "slug": "opinion_expert_comment",
        "title": "Expert Comment",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"news_input_path": blob_path},
        "meta": {"persona": "Expert"},
        "type": "opinion",
        "model": "gpt-4o-mini",
        "items": [top_item],
    }

    return write_outputs(args.section, args.date, args.league or "_", payload, status="ok")
