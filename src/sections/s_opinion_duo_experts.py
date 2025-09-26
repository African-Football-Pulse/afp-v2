# src/sections/s_opinion_duo_experts.py
import os
import yaml
from src.sections import utils
from src.producer.gpt import run_gpt


def load_duo_experts(lang: str):
    """Load expert1 and expert2 personas for given language from speaking_roles.yaml (local config)."""
    with open("config/speaking_roles.yaml", "r", encoding="utf-8") as f:
        roles_cfg = yaml.safe_load(f)

    duo_cfg = roles_cfg.get("roles", {}).get("duo_experts", {})
    if lang not in duo_cfg:
        raise ValueError(f"[duo_experts] No duo_experts defined for lang={lang}")

    return duo_cfg[lang]["expert1"], duo_cfg[lang]["expert2"]


def build_section(args):
    """Produce a duo-expert opinion section with GPT commentary."""

    # Extract arguments with fallbacks
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    pod = getattr(args, "pod", "default_pod")
    lang = getattr(args, "lang", "en")
    section_code = getattr(args, "section", "S.OPINION.DUO.EXPERTS")

    # Load scored_enriched candidates
    candidates = utils.load_scored_enriched(day, league=league)
    if not candidates:
        print(f"[opinion_duo] WARN: No candidates available for {day}")
        text = "No candidates available."
        payload = {
            "slug": "opinion_duo_experts",
            "title": "Duo Expert Opinion",
            "text": text,
            "length_s": 2,
            "sources": {},
            "meta": {"personas": []},
            "type": "opinion",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"personas": []}}
        return utils.write_outputs(
            section_code=section_code,
            day=day,
            league=league,
            lang=lang,
            pod=pod,
            manifest=manifest,
            status="empty",
            payload=payload,
        )

    # Pick two candidates
    first_item = candidates[0]
    second_item = candidates[1] if len(candidates) > 1 else candidates[0]
    news_text = f"- {first_item.get('text','')}\n- {second_item.get('text','')}"

    # Personas (expert duo)
