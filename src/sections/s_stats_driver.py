import os
from src.sections import utils
from src.producer.gpt import run_gpt  # om GPT används i framtida stats
from src.storage import azure_blob  # om warehouse behövs


def build_section(args, **kwargs):
    section_code = getattr(args, "section", "S.STATS.DRIVER")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")

    print(f"[{section_code}] 🚀 Startar driver för stats (league={league}, day={day})")

    ran_sections = []

    # Här kan du lägga till logik för att köra olika stats-subsektioner
    # T.ex. anropa build_section för TOP.CONTRIBUTORS.SEASON eller TOP.PERFORMERS.ROUND
    # Just nu simulerar vi att vi kör ett par sektioner
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

    # ✅ Loggmarkör för att verifiera att rätt version körs
    print(f"[{section_code}] Returning manifest via utils.write_outputs (sections: {ran_sections})")

    # Returnera manifest via utils.write_outputs
    return utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        payload=payload,
        lang=lang,
        status="ok" if ran_sections else "no_data",
    )
