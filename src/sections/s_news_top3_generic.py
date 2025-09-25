# src/sections/s_news_top3_generic.py
import os, json
from typing import List, Dict, Any

from src.storage import azure_blob
from src.sections import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def _load_scored_items(day: str) -> List[Dict[str, Any]]:
    """Försök ladda enriched scored först, annars fallback till scored"""
    enriched_path = f"producer/candidates/{day}/scored_enriched.jsonl"
    base_path = f"producer/candidates/{day}/scored.jsonl"

    path = enriched_path if azure_blob.exists(CONTAINER, enriched_path) else base_path
    if not azure_blob.exists(CONTAINER, path):
        print(f"[s_news_top3_generic] ❌ Hittar inte scored: {path}")
        return []

    print(f"[s_news_top3_generic] Loading scored items from {path}")
    text = azure_blob.get_text(CONTAINER, path)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def build_section(args):
    """Bygg Top 3 news-sektionen (globalt, enriched om möjligt)"""
    day = args.date
    league = args.league
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    pretty_league = league.replace("_", " ").title()
    section_title = f"Top 3 {pretty_league} News"

    print(f"[s_news_top3_generic] Bygger {section_title} @ {day}")
    items = _load_scored_items(day)
    if not items:
        payload = {
            "title": section_title,
            "text": f"No scored news items available for {pretty_league}.",
            "type": "news",
            "sources": {},
        }
        return utils.write_outputs("S.NEWS.TOP3", day, league, payload, status="empty", lang=lang)

    # Sortera på score
    items = sorted(items, key=lambda c: c.get("score", 0), reverse=True)

    # Ta topp 3
    top3, seen_players = [], set()
    for c in items:
        pname = c.get("player", {}).get("name")
        if pname in seen_players:
            continue
        top3.append(c)
        seen_players.add(pname)
        if len(top3) >= 3:
            break

    # Bygg markdown-innehåll
    lines = [f"### {section_title}", ""]
    for i, c in enumerate(top3, 1):
        player = c.get("player", {}).get("name", "Unknown")
        club = c.get("player", {}).get("club", "")
        title = c.get("title", "Untitled")
        url = c.get("source", {}).get("url", "")
        score = c.get("score", 0)

        # Välj GPT-input (article_text om enriched, annars summary)
        text_input = c.get("article_text") or c.get("summary") or ""

        lines.append(f"{i}. **{player} – {club}**")
        lines.append(f"   {title}")
        if text_input:
            lines.append(f"   {text_input[:200]}...")  # kort preview
        if url:
            lines.append(f"   ({url})")
        lines.append(f"   Score={score:.2f}")
        lines.append("")

    content = "\n".join(lines)

    payload = {
        "title": section_title,
        "text": content,
        "type": "news",
        "sources": {i: c.get("source", {}) for i, c in enumerate(top3, 1)},
    }

    return utils.write_outputs("S.NEWS.TOP3", day, league, payload, status="success", lang=lang)
