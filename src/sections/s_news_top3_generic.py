# src/sections/s_news_top3_generic.py
import os, json
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.sections.utils import write_outputs
from src.storage import azure_blob
from src.gpt import run_gpt
from src.producer import role_utils

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def _load_scored_items(league: str, day: str) -> List[Dict[str, Any]]:
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    path = f"scored/{day}/{league}/scored.jsonl"
    if not azure_blob.exists(container, path):
        return []
    text = azure_blob.get_text(container, path)
    return [json.loads(line) for line in text.splitlines() if line.strip()]

def _pick_topn_diverse(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    seen, picked = set(), []
    for it in sorted(items, key=lambda x: x.get("score", 0), reverse=True):
        player = it.get("player", {}).get("name")
        if player and player not in seen:
            seen.add(player)
            picked.append(it)
        if len(picked) >= n:
            break
    return picked

def build_section(args=None):
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    top_n = int(getattr(args, "top_n", 3))
    lang = getattr(args, "lang", "en")
    section_id = getattr(args, "section_code", "S.NEWS.TOP3")
    role = "news_anchor"  # från sections_library.yaml

    # Hämta pod-konfig
    pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
    pod_cfg = pods_cfg.get(getattr(args, "pod"))
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)

    # Hämta personas
    personas = role_utils.load_yaml("config/personas.json")
    persona_cfg = personas.get(persona_id, {})
    persona_block = f"""{persona_cfg.get("name")}
Role: {persona_cfg.get("role")}
Voice: {persona_cfg.get("voice")}
Tone: {persona_cfg.get("tone", {}).get("primary", "")}
Style: {persona_cfg.get("style", "")}
"""

    # Ladda nyheter
    items = _load_scored_items(league, day)
    if not items:
        payload = {
            "slug": "top3_news",
            "title": "Top 3 African Player News",
            "text": "No scored news items available.",
            "length_s": 2,
            "sources": [],
            "meta": {"role": role, "persona": persona_id},
            "items": [],
            "type": "news",
            "model": "gpt-4o-mini",
        }
        return write_outputs(section_id, day, league, payload, status="no_items", lang=lang)

    picked = _pick_topn_diverse(items, top_n)

    # GPT-sammanfattning för varje nyhet
    summaries = []
    for it in picked:
        url = it.get("source", {}).get("url")
        title = it.get("title")
        prompt_config = {
            "persona": persona_block,
            "instructions": f"""Summarize this football news item in 2–3 spoken-style sentences:
Title: {title}
URL: {url}"""
        }
        ctx = {"title": title, "url": url}
        system_rules = "You are an assistant generating natural spoken football news summaries."
        gpt_out = run_gpt(prompt_config, ctx, system_rules)
        summaries.append(f"- {title} ({it.get('source',{}).get('name')})\n  {gpt_out}")

    body = f"Top {len(picked)} African player news for {league} ({day}):\n" + "\n".join(summaries)

    payload = {
        "slug": "top3_news",
        "title": "Top 3 African Player News",
        "text": body,
        "length_s": len(picked) * 35,
        "sources": [it.get("source", {}).get("url") for it in picked],
        "meta": {"role": role, "persona": persona_id},
        "items": picked,
        "type": "news",
        "model": "gpt-4o-mini",
    }

    return write_outputs(section_id, day, league, payload, status="ok", lang=lang)
