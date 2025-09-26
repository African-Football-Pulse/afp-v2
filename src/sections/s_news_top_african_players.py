# src/sections/s_news_top_african_players.py
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.sections import utils
from src.sections.utils import write_outputs
from src.producer.gpt import run_gpt


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def _pick_top_players(candidates: List[Dict[str, Any]], top_n: int = 3) -> List[Dict[str, Any]]:
    """Pick top African players based on score (highest per unique player)."""
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
    Build a section highlighting top African players based on scored_enriched candidates.
    Writes section.json, section.md and section_manifest.json.
    """
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    pod = getattr(args, "pod", "default")
    section_code = getattr(args, "section", "S.NEWS.TOP_AFRICAN_PLAYERS")
    top_n = int(getattr(args, "top_n", 3))
    lang = getattr(args, "lang", "en")

    # Load scored_enriched candidates
    candidates = utils.load_scored_enriched(day)
    if not candidates:
        text = "No news items available."
        payload = {
            "slug": "top_african_players",
            "title": "Top African Players",
            "text": text,
            "length_s": 2,
            "sources": {},
            "meta": {"persona": "system"},
            "type": "news",
            "model": "static",
            "items": [],
        }
        return write_outputs(
            section_code=section_code,
            day=day,
            league=league,
            payload=payload,
            status="no_candidates",
            lang=lang,
        )

    picked = _pick_top_players(candidates, top_n=top_n)

    # Persona (analyst/expert)
    persona_id, persona_block = utils.get_persona_block("analyst", pod)

    # Build GPT prompt
    players_list = ", ".join([c.get("player", {}).get("name", "?") for c in picked])
    pretty_league = league.replace("_", " ").title()
    instructions = (
        f"You are a football analyst. Write a {lang} script highlighting "
        f"the top {len(picked)} African players today in the {pretty_league}. "
        f"The players are: {players_list}. "
        f"Explain briefly why they stood out, in 3–5 sentences overall. "
        f"Do not include scores, raw URLs or metadata."
    )
    prompt_config = {
        "persona": persona_block,
        "instructions": instructions,
    }

    enriched_text = run_gpt(prompt_config, {"players": picked}, system_rules=None)

    # Build payload
    payload = {
        "slug": "top_african_players",
        "title": "Top African Players",
        "text": enriched_text,
        "length_s": len(picked) * 30,  # approx length
        "sources": {},
        "meta": {
            "persona": persona_id,
            "players": [c.get("player", {}).get("name") for c in picked],
            "league": league,
            "day": day,
        },
        "type": "news",
        "model": "gpt",
        "items": picked,
    }

    return write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        payload=payload,
        status="ok",
        lang=lang,
    )
