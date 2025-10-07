# src/sections/s_opinion_expert_comment.py
import os
import yaml
from src.sections import utils
from src.producer.gpt import run_gpt


def load_roles():
    """Load speaking roles for single_expert from config/speaking_roles.yaml (local config)."""
    with open("config/speaking_roles.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_section(args):
    """Produce a single-expert opinion section with GPT commentary."""

    # Extract arguments with fallbacks
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    pod = getattr(args, "pod", "default_pod")
    lang = getattr(args, "lang", "en")  # expected: "en", "sw", "ar"
    section_code = getattr(args, "section", "S.OPINION.EXPERT.COMMENT")

    # Load candidates
    candidates = utils.load_scored_enriched(day, league=league)
    if not candidates:
        print(f"[opinion_expert] WARN: No candidates available for {day}")
        text = "No candidates available."
        payload = {
            "slug": "opinion_expert_comment",
            "title": "Expert Comment",
            "text": text,
            "length_s": 2,
            "sources": {},
            "meta": {"persona": "system"},
            "type": "opinion",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": "system"}}
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

    # Take top candidate
    top_item = candidates[0]
    news_text = top_item.get("text", "")

    # Load role mapping for single_expert
    roles = load_roles()
    expert_id = roles.get("roles", {}).get("single_expert", {}).get(lang, "expert")

    # GPT setup
    instructions = (
        f"Write a short (~120 words) spoken-style expert comment in {lang} about this news:\n\n{news_text}\n\n"
        f"Stay natural, insightful, and record-ready. Present it as {expert_id.upper()} speaking."
        f"Do not add generic openings like 'Welcome'."
        f"Do not add generic closings like 'Stay tuned'."
    )
    prompt_config = {
        "persona": f"single_expert:{expert_id}",
        "instructions": instructions,
    }
    ctx = {"candidates": [top_item]}
    system_rules = "You are an assistant generating natural spoken-style football commentary."

    gpt_output = run_gpt(prompt_config, ctx, system_rules)

    payload = {
        "slug": "opinion_expert_comment",
        "title": "Expert Comment",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {},
        "meta": {
            "persona": expert_id,
        },
        "type": "opinion",
        "model": "gpt",
        "items": [top_item],
    }

    manifest = {"script": gpt_output, "meta": {"persona": expert_id}}

    return utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        lang=lang,
        pod=pod,
        manifest=manifest,
        status="success",
        payload=payload,
    )
