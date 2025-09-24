# src/sections/s_news_club_highlight.py
from datetime import datetime, timezone
from collections import Counter
import os, json

from src.sections.utils import write_outputs, load_candidates
from src.gpt import run_gpt
from src.storage import azure_blob
from src.producer import role_utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def pick_best_club(candidates, exclude_club=None):
    counts, clubs = Counter(), {}
    for c in candidates:
        club = c.get("player", {}).get("club")
        if not club:
            continue
        counts[club] += 1
        clubs.setdefault(club, []).append(c)
    if not counts:
        return None, []
    for club, _ in counts.most_common():
        if club != exclude_club:
            return club, clubs[club]
    return None, []

def estimate_length(text: str, target: int) -> int:
    return min(max(int(len(text.split()) / 2.6), target - 10), target + 10)

def load_last_club(day: str):
    path = "sections/state/last_club.json"
    try:
        data = azure_blob.get_json(CONTAINER, path)
        if data and data.get("date") != day:
            return data.get("club")
    except Exception:
        pass
    return None

def save_last_club(day: str, club: str):
    path = "sections/state/last_club.json"
    azure_blob.upload_json(CONTAINER, path, {"date": day, "club": club})

def build_section(args=None):
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    lang = getattr(args, "lang", "en")
    section_id = getattr(args, "section_code", "S.NEWS.CLUB_HIGHLIGHT")
    target = int(getattr(args, "target_length_s", 50))
    role = "news_anchor"  # från sections_library.yaml

    # Kandidater
    candidates, blob_path = load_candidates(day, args.news[0] if hasattr(args, "news") and args.news else None)
    if not candidates:
        payload = {
            "slug": "club_highlight",
            "title": "Club Spotlight",
            "text": "No club highlights available.",
            "length_s": 2,
            "sources": [],
            "meta": {"role": role, "persona": "AK"},
            "type": "news",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_candidates", lang=lang)

    last_club = load_last_club(day)
    club, picked = pick_best_club(candidates, exclude_club=last_club)
    if not club:
        payload = {
            "slug": "club_highlight",
            "title": "Club Spotlight",
            "text": "No club highlights available.",
            "length_s": 2,
            "sources": [],
            "meta": {"role": role, "persona": "AK"},
            "type": "news",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_items", lang=lang)

    # 🎯 Roll + persona lookup
    pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
    pod_cfg = pods_cfg.get(getattr(args, "pod"))
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)

    personas = role_utils.load_yaml("config/personas.json")
    persona_cfg = personas.get(persona_id, {})
    persona_block = f"""{persona_cfg.get("name")}
Role: {persona_cfg.get("role")}
Voice: {persona_cfg.get("voice")}
Tone: {persona_cfg.get("tone", {}).get("primary", "")}
Style: {persona_cfg.get("style", "")}
"""

    # GPT-prompt
    headlines = "\n".join([f"- {c['source']['name']}: {c['source']['url']}" for c in picked[:3]])
    prompt_config = {
        "persona": persona_block,
        "instructions": f"""Give a lively spoken-style club spotlight (~150 words, ~45–60s) for {club}.
Base it on these sources:
{headlines}

Make it conversational, engaging, and record-ready."""
    }
    ctx = {"club": club, "candidates": picked}
    system_rules = "You are an assistant generating natural spoken-style football commentary."

    gpt_output = run_gpt(prompt_config, ctx, system_rules)
    length_s = estimate_length(gpt_output, target)

    payload = {
        "slug": "club_highlight",
        "title": f"{club} spotlight",
        "text": gpt_output,
        "length_s": length_s,
        "sources": [c["source"]["url"] for c in picked],
        "meta": {"role": role, "persona": persona_id},
        "type": "news",
        "model": "gpt-4o-mini",
        "items": picked,
    }

    save_last_club(day, club)
    return write_outputs(section_id, day, league, payload, status="ok", lang=lang)
