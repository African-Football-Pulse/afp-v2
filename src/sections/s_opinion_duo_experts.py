import os, json
from src.gpt import run_gpt
from src.sections.utils import write_outputs
from src.storage import azure_blob

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")

def build_section(args):
    """Produce a duo-expert opinion section with GPT commentary."""
    blob_path = args.news[0] if args.news else f"producer/candidates/{args.date}/scored.jsonl"

    try:
        text = azure_blob.get_text(CONTAINER, blob_path)
        candidates = [json.loads(line) for line in text.splitlines() if line.strip()]
    except Exception as e:
        print(f"[opinion_duo] WARN: could not load from Azure → {blob_path} ({e})")
        payload = {
            "slug": "opinion_duo_experts",
            "title": "Duo Expert Opinion",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {"news_input_path": blob_path},
            "meta": {"persona": "Expert Duo"},
            "type": "opinion",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(args.section, args.date, args.league or "_", payload, status="no_candidates")

    if not candidates:
        print(f"[opinion_duo] WARN: empty candidates in {blob_path}")
        payload = {
            "slug": "opinion_duo_experts",
            "title": "Duo Expert Opinion",
            "text": "No candidates available.",
            "length_s": 2,
            "sources": {"news_input_path": blob_path},
            "meta": {"persona": "Expert Duo"},
            "type": "opinion",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(args.section, args.date, args.league or "_", payload, status="no_candidates")

    # Välj två kandidater
    first_item = candidates[0]
    second_item = candidates[1] if len(candidates) > 1 else candidates[0]
    news_text = f"- {first_item.get('text','')}\n- {second_item.get('text','')}"

    # GPT setup
    prompt_config = {
        "persona": "Two African football experts debating",
        "instructions": f"Have a lively, 2-voice exchange (~140 words total) based on these stories:\n\n{news_text}\n\nMake it conversational, record-ready, and avoid lists or placeholders."
    }
    ctx = {"candidates": [first_item, second_item]}
    system_rules = "You are an assistant generating a natural spoken-style football dialogue."

    gpt_output = run_gpt(prompt_config, ctx, system_rules)

    payload = {
        "slug": "opinion_duo_experts",
        "title": "Duo Expert Opinion",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"news_input_path": blob_path},
        "meta": {"persona": "Expert Duo"},
        "type": "opinion",
        "model": "gpt-4o-mini",
        "items": [first_item, second_item],
    }

    return write_outputs(args.section, args.date, args.league or "_", payload, status="ok")
