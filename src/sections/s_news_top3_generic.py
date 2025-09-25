# src/sections/s_news_top3_generic.py
import os, json
from typing import List, Dict, Any

from src.storage import azure_blob
from src.sections import utils
from src.producer.gpt import run_gpt

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def _load_scored_items(day: str) -> List[Dict[str, Any]]:
    """Försök ladda enriched scored först, annars fallback till scored"""
    enriched_path = f"producer/scored/{day}/scored_enriched.jsonl"
    base_path = f"producer/scored/{day}/scored.jsonl"

    path = enriched_path if azure_blob.exists(CONTAINER, enriched_path) else base_path
    if not azure_blob.exists(CONTAINER, path):
        print(f"[s_news_top3_generic] ❌ Hittar inte scored: {path}")
        return []

    print(f"[s_news_top3_generic] Loading scored items from {path}")
    text = azure_blob.get_text(CONTAINER, path)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def build_section(args):
    """Bygg Top 3 news-sektionen via GPT"""
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

    # Sortera på score och ta topp 3 (unika spelare)
    items = sorted(items, key=lambda c: c.get("score", 0), reverse=True)
    top3, seen_players = [], set()
    for c in items:
        pname = c.get("player", {}).get("name")
        if pname in seen_players:
            continue
        top3.append(c)
        seen_players.add(pname)
        if len(top3) >= 3:
            break

    # GPT-prompt
    prompt_config = {
    "persona": "news_anchor",
    "instructions": (
        f"You are a sports news anchor. Write a flowing news script in {lang} "
        f"about the following top 3 {pretty_league} stories. "
        f"Each story should be 2–3 sentences, engaging and clear, "
        f"mentioning the player and club naturally. "
        f"Do not include scores, raw URLs or metadata. "
        f"Do not add generic closing lines like 'Stay tuned' or 'That's all for today'."
    )
    }
    ctx = {"articles": top3}
    system_rules = "You are a professional football journalist creating spoken news scripts."

    gpt_text = run_gpt(prompt_config, ctx, system_rules)

    payload = {
        "title": section_title,
        "text": gpt_text,
        "type": "news",
        "sources": {i: c.get("source", {}) for i, c in enumerate(top3, 1)},
    }

    return utils.write_outputs("S.NEWS.TOP3", day, league, payload, status="success", lang=lang)
