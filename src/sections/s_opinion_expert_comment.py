# src/sections/s_opinion_expert_comment.py
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict

from src.storage import azure_blob
from src.sections import gpt  # GPT wrapper

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a single speaker monologue that lasts ~45 seconds (≈120–160 words).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona block.
- Fold in news facts without inventing specifics.
- No placeholders like [TEAM]; use only info present in the news input.
- Avoid list formats; deliver a flowing, spoken monologue.
- Keep it record-ready: natural pacing, light rhetorical devices, 1–2 short pauses (…).
- End with a crisp takeaway line.
"""


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def _load_json(path: str):
    if not path:
        return {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return azure_blob.get_json(CONTAINER, path)


def _load_news(path: str):
    try:
        return _load_json(path)
    except Exception:
        return []


def _build_section(
    section_code: str,
    news_path: str,
    personas_path: str,
    persona_id: str,
    date: str,
    league: str,
    topic: str,
) -> Dict[str, Any]:
    news_items = _load_news(news_path)
    if not news_items:
        return {
            "section_id": section_code,
            "date": date,
            "league": league,
            "status": "no_news",
            "text": "",
        }

    # Välj det högst scorade itemet
    top_item = max(news_items, key=lambda x: x.get("score", 0))
    player = top_item.get("player") or ", ".join(top_item.get("entities", {}).get("players", []))
    title = top_item.get("title", "")

    # Slå upp persona
    personas = _load_json(personas_path)
    persona_block = personas.get(persona_id, {"name": "Generic Expert", "style": "Analytical"})

    # GPT prompt
    prompt = (
        f"{SYSTEM_RULES}\n\n"
        f"Persona:\n{json.dumps(persona_block, indent=2)}\n\n"
        f"News fact:\nTitle: {title}\nPlayer: {player}\nSource: {top_item.get('source')}\n\n"
        "Generate the monologue in English."
    )

    script_text = gpt.run(prompt)

    return {
        "section_id": section_code,
        "date": date,
        "league": league,
        "status": "ok",
        "persona": persona_block,
        "player": player,
        "topic": topic,
        "item": top_item,
        "text": script_text,
    }


def build_section(args):
    return _build_section(
        section_code=args.section_code,
        news_path=args.news,
        personas_path=getattr(args, "personas", "config/personas.json"),
        persona_id=getattr(args, "persona_id", "AK"),  # default Ama K
        date=getattr(args, "date", today_str()),
        league=getattr(args, "league", "premier_league"),
        topic=getattr(args, "topic", "_"),
    )
