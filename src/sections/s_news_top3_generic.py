# src/sections/s_news_top3_generic.py
import os
from datetime import datetime, timezone
from typing import Any, Dict, List
import json

from src.sections.utils import write_outputs
from src.storage import azure_blob

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def _load_scored_items(league: str, day: str) -> List[Dict[str, Any]]:
    """Ladda scored.jsonl för given dag och liga från Azure."""
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    if not container or not container.strip():
        raise RuntimeError("AZURE_STORAGE_CONTAINER is missing or empty")

    path = f"scored/{day}/{league}/scored.jsonl"
    if not azure_blob.exists(container, path):
        print(f"[s_news_top3_generic] ⚠️ scored.jsonl saknas: {path}")
        return []

    text = azure_blob.get_text(container, path)
    items = []
    for line in text.splitlines():
        if line.strip():
            try:
                items.append(json.loads(line))
            except Exception as e:
                print(f"[s_news_top3_generic] JSON decode error: {e}")
    return items

def _pick_topn_diverse(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    """Välj top N items med högsta score, max ett per spelare."""
    seen_players, picked = set(), []
    for it in items:
        player = it.get("player", {}).get("name")
        if not player:
            continue
        if player in seen_players:
            continue
        seen_players.add(player)
        picked.append(it)
        if len(picked) >= n:
            break
    return picked

def _render_section(league: str, day: str, items: List[Dict[str, Any]]) -> str:
    lines = [f"Top {len(items)} African player news for {league} ({day}):"]
    for it in items:
        title = it.get("title") or ""
        src = it.get("source", {}).get("name") if isinstance(it.get("source"), dict) else it.get("source", "")
        lines.append(f"- {title} ({src})")
    return "\n".join(lines)

def build_section(args=None):
    """Bygg en sektion för topp 3 nyheter från scored.jsonl (med diversitet)."""
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    top_n = int(getattr(args, "top_n", 3))
    lang = getattr(args, "lang", "en")
    section_id = getattr(args, "section_code", "S.NEWS.TOP3")

    # Hämta role från args eller default till news_anchor
    role = getattr(args, "role", "news_anchor")

    items = _load_scored_items(league, day)
    if not items:
        print("[s_news_top3_generic] Inga scored items hittades")
        payload = {
            "slug": "top3_news",
            "title": "Top 3 African Player News",
            "text": "No scored news items available.",
            "length_s": 2,
            "sources": {"feeds": []},
            "meta": {"role": role},
            "items": [],
            "type": "news",
            "model": "gpt-4o-mini",
        }
        return write_outputs(section_id, day, league, payload, status="no_items", lang=lang)

    # Sortera på score (högst först)
    items_sorted = sorted(items, key=lambda x: x.get("score", 0), reverse=True)

    # Välj diversifierat top-N
    picked = _pick_topn_diverse(items_sorted, top_n)
    body = _render_section(league, day, picked)

    print(f"[s_news_top3_generic] Totalt {len(items)} scored → valde {len(picked)}")

    payload = {
        "slug": "top3_news",
        "title": "Top 3 African Player News",
        "text": body,
        "length_s": len(picked) * 30,
        "sources": {"feeds": []},
        "meta": {"role": role},
        "items": picked,
        "type": "news",
        "model": "gpt-4o-mini",
    }

    return write_outputs(section_id, day, league, payload, status="ok", lang=lang)
