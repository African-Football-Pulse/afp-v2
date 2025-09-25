# src/sections/s_news_top3_generic.py
import os, json
from typing import List, Dict, Any

from src.storage import azure_blob
from src.sections import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def _load_scored_items(day: str) -> List[Dict[str, Any]]:
    """Ladda global scored-lista från producer/scored"""
    path = f"producer/scored/{day}/scored.jsonl"
    if not azure_blob.exists(CONTAINER, path):
        print(f"[s_news_top3_generic] ❌ Hittar inte scored: {path}")
        return []
    text = azure_blob.get_text(CONTAINER, path)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _filter_by_league(items: List[Dict[str, Any]], league: str) -> List[Dict[str, Any]]:
    """Filtrera scored items till de som hör till given liga (via player.league_key)"""
    league_items = []
    for c in items:
        player = c.get("player")
        if not player:
            continue
        if player.get("league_key") == league:
            league_items.append(c)
    return league_items


def build_section(args):
    """Bygg Top 3 news-sektionen för given liga"""
    day = args.date
    league = args.league
    lang = getattr(args, "lang", "en")          # fallback till engelska
    pod = getattr(args, "pod", "default_pod")  # fallback till default_pod

    print(f"[s_news_top3_generic] Bygger Top3 för {league} @ {day}")
    items = _load_scored_items(day)
    if not items:
        payload = {
            "title": "Top 3 African Player News",
            "text": "No scored news items available.",
            "type": "news",
            "sources": {},
        }
        return utils.write_outputs("S.NEWS.TOP3", day, league, payload, status="empty", lang=lang)

    # Filtrera på liga
    items = _filter_by_league(items, league)
    if not items:
        payload = {
            "title": "Top 3 African Player News",
            "text": f"No scored news items for league {league}.",
            "type": "news",
            "sources": {},
        }
        return utils.write_outputs("S.NEWS.TOP3", day, league, payload, status="empty", lang=lang)

    # Sortera på score (fallande)
    items = sorted(items, key=lambda c: c.get("score", 0), reverse=True)

    # Ta topp 3, försök diversifiera spelare
    top3 = []
    seen_players = set()
    for c in items:
        pname = c.get("player", {}).get("name")
        if pname in seen_players:
            continue
        top3.append(c)
        seen_players.add(pname)
        if len(top3) >= 3:
            break

    # Bygg markdown-innehåll
    lines = ["### Top 3 African Player News", ""]
    for i, c in enumerate(top3, 1):
        headline = c.get("title", "Untitled")
        player = c.get("player", {}).get("name", "Unknown")
        source = c.get("source", {}).get("name", "")
        score = c.get("score", 0)
        lines.append(f"{i}. **{headline}** ({player}, {source}, score={score:.2f})")

    content = "\n".join(lines)

    payload = {
        "title": "Top 3 African Player News",
        "text": content,
        "type": "news",
        "sources": {i: c.get("source", {}) for i, c in enumerate(top3, 1)},
    }

    return utils.write_outputs("S.NEWS.TOP3", day, league, payload, status="success", lang=lang)
