# src/sections/s_news_club_highlight.py
from datetime import datetime, timezone
from collections import Counter
from typing import Any, Dict, List
import os

from src.sections.utils import write_outputs, load_news_items
from src.gpt import run_gpt

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def load_all_items(league: str, day: str) -> List[Dict[str, Any]]:
    """Hämta alla nyhetsitems för en liga/dag från curated/news/"""
    feeds = [
        "guardian_football",
        "bbc_football",
        "sky_sports_premier_league",
        "independent_football",
        "football_london_all",
    ]
    items: List[Dict[str, Any]] = []
    for feed in feeds:
        feed_items = load_news_items(feed, league, day)
        items.extend(feed_items)
    return items


def pick_best_club(items: List[Dict[str, Any]]):
    """Räkna klubbomnämnanden och välj den med flest träffar."""
    counts = Counter()
    clubs = {}
    for it in items:
        for club in it.get("entities", {}).get("clubs", []):
            counts[club] += 1
            clubs.setdefault(club, []).append(it)
    if not counts:
        return None, []
    best = counts.most_common(1)[0][0]
    return best, clubs[best]


def estimate_length(text: str, target: int) -> int:
    """Enkel approx av längd i sekunder."""
    return min(max(int(len(text.split()) / 2.6), target - 10), target + 10)


def build_section(args=None):
    """Bygg en GPT-driven klubbhighlight-sektion."""
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    lang = getattr(args, "lang", "en")
    section_id = getattr(args, "section_code", "S.NEWS.CLUB_HIGHLIGHT")
    target = int(getattr(args, "target_length_s", 50))

    items = load_all_items(league, day)
    club, picked = pick_best_club(items)

    if not club:
        payload = {
            "slug": "club_highlight",
            "title": "Club Spotlight",
            "text": "No club highlights available.",
            "length_s": 2,
            "sources": [],
            "meta": {"persona": "Ama K"},
            "type": "news",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_items", lang=lang)

    # GPT-setup
    titles = "\n".join([f"- {it.get('title')} ({it.get('source')})" for it in picked[:3]])
    prompt_config = {
        "persona": "African football news anchor",
        "instructions": f"""Give a lively spoken-style club spotlight (~150 words, about 45–60s) for {club}.
Base it on these headlines:
{titles}

Make it conversational, engaging, and record-ready."""
    }
    ctx = {"club": club, "items": picked}
    system_rules = "You are an assistant generating natural spoken-style football commentary."

    gpt_output = run_gpt(prompt_config, ctx, system_rules)
    length_s = estimate_length(gpt_output, target)

    payload = {
        "slug": "club_highlight",
        "title": f"{club} spotlight",
        "text": gpt_output,
        "length_s": length_s,
        "sources": [i.get("id") for i in picked],
        "meta": {"persona": "Ama K"},
        "type": "news",
        "model": "gpt-4o-mini",
        "items": picked,
    }
    return write_outputs(section_id, day, league, payload, status="ok", lang=lang)
