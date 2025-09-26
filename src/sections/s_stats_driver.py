import os
from src.sections import utils


def build_section(args, **kwargs):
    section_code = getattr(args, "section", "S.STATS.DRIVER")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")

    print(f"[{section_code}] 🚀 Startar driver för stats (league={league}, day={day})")

    ran_sections = []

    # Simulerar körning av subsektioner
    try:
        print(f"[{section_code}] Kör S.STATS.TOP.CONTRIBUTORS.SEASON")
        ran_sections.append("S.STATS.TOP.CONTRIBUTORS.SEASON")
    except Exception as e:
        print(f"[{section_code}] ❌ Fel i S.STATS.TOP.CONTRIBUTORS.SEASON: {e}")

    try:
        print(f"[{section_code}] Kör S.STATS.TOP.PERFORMERS.ROUND")
        ran_sections.append("S.STATS.TOP.PERFORMERS.ROUND")
    except Exception as e:
        print(f"[{section_code}] ❌ Fel i S.STATS.TOP.PERFORMERS.ROUND: {e}")

    # Bygg payload med resultat från körda stats-sektioner
    payload = {
        "slug": "stats_driver",
        "title": "Stats driver summary",
        "text": f"Ran {len(ran_sections)} stats sections: {', '.join(ran_sections)}",
        "items": ran_sections,
    }

    # ✅ Viktigt: returnera manifestet från write_outputs, inte payload
    manifest = utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        payload=payload,
        lang=lang,
        status="ok" if ran_sections else "no_data",
    )

    print(f"[{section_code}] ✅ Returnerar manifest: {manifest.keys()}")
    return manifest
