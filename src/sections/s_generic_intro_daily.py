# src/sections/s_generic_intro_daily.py
import os
from datetime import datetime
from src.sections import utils


def build_section(args) -> dict:
    """Produce a daily intro section"""

    # Extract arguments with fallbacks
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    # Section code
    section_code = getattr(args, "section", "S.GENERIC.INTRO_DAILY")

    # Get persona dynamically (news anchor)
    persona_id, persona_block = utils.get_persona_block("news_anchor", pod)

    # Format date
    try:
        dt = datetime.strptime(day, "%Y-%m-%d")
        date_str = dt.strftime("%B %d, %Y")
    except Exception:
        date_str = day

    text = (
        f"Welcome to African Football Pulse! "
        f"It’s {date_str}, and this is your daily Premier League update. "
        f"We’ll bring you the latest headlines, key talking points, "
        f"and stories that matter most to African fans."
    )

    payload = {
        "slug": "intro_daily",
        "title": "Daily Intro",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {},
        "meta": {"persona": persona_id},
        "type": "generic",
        "model": "static",
    }

    manifest = {
        "script": text,
        "meta": {"persona": persona_id},
    }

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
