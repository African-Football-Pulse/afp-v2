import os
from datetime import datetime
from src.sections import utils


def build_section(args) -> dict:
    """Produce a generic post-match outro section"""

    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    section_code = getattr(args, "section", "S.GENERIC.OUTRO.POSTMATCH")

    persona_id, _ = utils.get_persona_block("news_anchor", pod)

    outro_map = {
        "en": (
            "That wraps up this weekend’s post-match coverage. "
            "Thanks for tuning in to African Football Pulse — "
            "we’ll be back soon with more stories from across the Premier League."
        ),
        "sw": (
            "Hapo ndipo tunamalizia mazungumzo ya mechi za wikendi hii. "
            "Asante kwa kusikiliza African Football Pulse — "
            "tutarudi hivi karibuni na hadithi zaidi kutoka Ligi Kuu ya Uingereza."
        ),
        "ar": (
            "وهكذا نختتم تغطية مباريات نهاية الأسبوع. "
            "شكرًا لانضمامكم إلى African Football Pulse — "
            "سنعود قريبًا بالمزيد من القصص من الدوري الإنجليزي الممتاز."
        ),
    }

    text = outro_map.get(lang, outro_map["en"])

    payload = {
        "slug": "outro_postmatch",
        "title": "Post-Match Outro",
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
