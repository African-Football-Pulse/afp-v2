import os
from datetime import datetime
from src.sections import utils


def build_section(args) -> dict:
    """Produce a generic daily outro section"""

    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    section_code = getattr(args, "section", "S.GENERIC.OUTRO.DAILY")

    persona_id, _ = utils.get_persona_block("news_anchor", pod)

    outro_map = {
        "en": (
            "That’s all for today’s recap. "
            "Thanks for listening — and see you tomorrow on African Football Pulse!"
        ),
        "sw": (
            "Hayo ndiyo kwa leo. "
            "Asante kwa kusikiliza — tukutane tena kesho kwenye African Football Pulse!"
        ),
        "ar": (
            "هذا كل شيء لليوم. "
            "شكرًا لاستماعكم — نراكم غدًا في African Football Pulse."
        ),
    }

    text = outro_map.get(lang, outro_map["en"])

    payload = {
        "slug": "outro_daily",
        "title": "Daily Outro",
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
        "target_duration_s": payload["length_s"],
        "role": "news_anchor",
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
