import os
from src.sections import utils

def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.PROJECT.STATUS")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    persona_id, _ = utils.get_persona_block("storyteller", pod)

    # ✅ Statisk text istället för parquet-fil
    text = (
        "We are currently tracking 50 African players in detail, "
        "and will soon expand with 30 more talents in the pipeline."
    )

    payload = {
        "slug": "stats_project_status",
        "title": "Project Status",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": "static"},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "static",
        "items": [],
    }
    manifest = {"script": text, "meta": {"persona": persona_id}}

    return utils.write_outputs(
        section_code, day, league, lang, pod, manifest, "success", payload
    )
