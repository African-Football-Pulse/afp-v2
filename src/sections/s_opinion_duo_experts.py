# src/sections/s_opinion_duo_experts.py
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.storage import azure_blob
from src.sections import gpt

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a short dialogue between two pundits (~45–60 seconds, 6–8 exchanges).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona blocks.
- Fold in news facts without inventing specifics.
- No placeholders like [TEAM]; use only info present in the news input.
- Keep it conversational, natural pacing, a couple of short pauses (…).
- Each speaker’s line should be 1–3 sentences.
- End with a crisp, shared takeaway.
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


def _load_news(path: str) -> List[Dict[str, Any]]:
    try:
        return _load_json(path)
    except Exception:
        return []


def _build_section(
    section_code: str,
    news_path: str,
    personas_path: str,
    persona_ids: List[str],
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

    # Välj topp-2 items
    sorted_items = sorted(news_items, key=lambda x: x.get("score", 0), reverse=True)
    top_items = sorted_items[:2]
    players = [i.get("player") or ", ".join(i.get("entities", {}).get("players", [])) for i in top_items]
    titles = [i.get("title", "") for i in top_items]

    # Slå upp personas
    personas = _load_json(personas_path)
    duo_blocks = [personas.get(pid.strip()) for pid in persona_ids if pid.strip() in personas]

    # GPT prompt
    prompt = (
        f"{SYSTEM_RULES}\n\n"
        f"Personas:\n{json.dumps(duo_blocks, indent=2)}\n\n"
        f"News facts:\n"
        + "\n".join([f"- {p}: {t}" for p, t in zip(players, titles)]) +
        "\n\nNow write the pundit dialogue."
    )

    script_text = gpt.run(prompt)

    return {
        "section_id": section_code,
        "date": date,
        "league": league,
        "status": "ok",
        "personas": duo_blocks,
        "players": players,
        "topics": titles,
        "items": top_items,
        "text": script_text,
    }


def build_section(args):
    persona_ids = getattr(args, "persona_ids", "AK,JJK").split(",")  # default Ama K & Coach JJ
    return _build_section(
        section_code=args.section_code,
        news_path=args.news,
        personas_path=getattr(args, "personas", "config/personas.json"),
        persona_ids=persona_ids,
        date=getattr(args, "date", today_str()),
        league=getattr(args, "league", "premier_league"),
        topic=getattr(args, "topic", "_"),
    )
