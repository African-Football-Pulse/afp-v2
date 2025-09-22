# src/sections/s_opinion_expert_comment.py
import os, json
from src.gpt import run_gpt
from src.sections.utils import write_outputs
from src.storage import azure_blob

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")

def build_section(args):
    """Produce a single-expert opinion section with GPT commentary."""
    blob_path = args.news[0] if args.news else f"producer/candidates/{args.date}/scored.jsonl"

    try:
        text = azure_blob.get_text(CONTAINER, blob_path)
        candidates = [json.loads(line) for line in text.splitlines() if line.strip()]
    except Exception as e:
        print(f"[opinion_expert] WARN: could not load from Azure → {blob_path} ({e})")
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

    if not candidates:
        print(f"[opinion_expert] WARN: empty candidates in {blob_path}")
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

    top_item = candidates[0]
    news_text = top_item.get("text", "")

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
        "sources": {"news_input_path": blob_path},
        "meta": {"persona": "Expert"},
        "type": "opinion",
        "model": "gpt-4o-mini",
        "items": [top_item],
    }

    return write_outputs(args.section, args.date, args.league or "_", payload, status="ok")
