# src/sections/s_news_top_african_players.py
from datetime import datetime, timezone
from typing import Any, Dict, List
import os
import json

from src.storage import azure_blob
from src.sections.utils import write_outputs

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def _load_candidates(path: str) -> List[Dict[str, Any]]:
    """Ladda kandidater från blob eller lokalt beroende på path."""
    try:
        if path.startswith("producer/"):
            return azure_blob.get_json(CONTAINER, path)
        else:
            with open(path, "r", encoding="utf-8") as f:
                return [json.loads(line) for line in f if line.strip()]
    except Exception:
        return []


def _pick_top_players(candidates: List[Dict[str, Any]], top_n: int = 3) -> List[Dict[str, Any]]:
    """Plocka toppspelare baserat på score."""
    sorted_cands = sorted(
        candidates,
        key=lambda c: c.get("score", 0.0),
        reverse=True,
    )
    picked, seen = [], set()
    for c in sorted_cands:
        player = c.get("player", {}).get("name")
        if not player or player in seen:
            continue
        seen.add(player)
        picked.append(c)
        if len(picked) >= top_n:
            break
    return picked


def build_section(args=None):
    """
    Bygg en sektion med topp-afrikanska spelare baserat på news-kandidater.
    Skriver ut section.json, section.md och section_manifest.json i mappstruktur.
    Returnerar ett manifest (dict).
    """
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    section_id = getattr(args, "section_code", "S.NEWS.TOP_AFRICAN_PLAYERS")
    top_n = int(getattr(args, "top_n", 3))
    news_path = None
    if hasattr(args, "news") and args.news:
        news_path = args.news[0]

    candidates: List[Dict[str, Any]] = []
    if news_path:
        candidates = _load_candidates(news_path)

    if not candidates:
        text = "No news items available."
        payload = {
            "slug": "top_african_players",
            "title": "Top African Players this week",
            "text": text,
            "length_s": 2,
            "sources": {"news_input_path": news_path},
            "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
            "type": "news",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_candidates")

    picked = _pick_top_players(candidates, top_n=top_n)

    # Bygg text
    lines = [f"Top {len(picked)} African players in {league} ({day}):"]
    for c in picked:
        player = c.get("player", {}).get("name")
        club = c.get("player", {}).get("club")
        score = c.get("score", 0.0)
        lines.append(f"- {player} ({club}), score={score:.2f}")
    body = "\n".join(lines)

    payload = {
        "slug": "top_african_players",
        "title": "Top African Players this week",
        "text": body,
        "length_s": len(picked) * 30,  # antag ca 30 sekunder per spelare
        "sources": {"news_input_path": news_path},
        "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
        "type": "news",
        "model": "gpt-4o-mini",
        "items": picked,
    }

    return write_outputs(section_id, day, league, payload, status="ok")
