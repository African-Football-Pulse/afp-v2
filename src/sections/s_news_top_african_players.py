# src/sections/s_news_top_african_players.py
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.sections.utils import write_outputs, load_candidates

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

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
    Bygg en sektion med topp-afrikanska spelare baserat på scored candidates.
    Skriver ut section.json, section.md och section_manifest.json i mappstruktur.
    """
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    section_id = getattr(args, "section_code", "S.NEWS.TOP_AFRICAN_PLAYERS")
    top_n = int(getattr(args, "top_n", 3))

    candidates, blob_path = load_candidates(day, args.news[0] if hasattr(args, "news") and args.news else None)

    if not candidates:
        text = "No news items available."
        payload = {
            "slug": "top_african_players",
            "title": "Top African Players this week",
            "text": text,
            "length_s": 2,
            "sources": {"news_input_path": blob_path},
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
        "sources": {"news_input_path": blob_path},
        "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
        "type": "news",
        "model": "gpt-4o-mini",
        "items": picked,
    }

    return write_outputs(section_id, day, league, payload, status="ok")
