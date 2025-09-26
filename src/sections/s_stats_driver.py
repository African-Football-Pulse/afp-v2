# src/sections/s_stats_driver.py
import os
from src.sections import utils

def build_section(args, **kwargs):
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")

    print(f"[S.STATS.DRIVER] 🚀 Startar driver för stats (league={league}, day={day})")

    # Kör undersektioner (dummy-exempel)
    try:
        print("[S.STATS.DRIVER] Kör S.STATS.TOP.CONTRIBUTORS.SEASON")
    except Exception as e:
        print(f"[S.STATS.DRIVER] ❌ Fel i S.STATS.TOP.CONTRIBUTORS.SEASON: {e}")

    try:
        print("[S.STATS.DRIVER] Kör S.STATS.TOP.PERFORMERS.ROUND")
    except Exception as e:
        print(f"[S.STATS.DRIVER] ❌ Fel i S.STATS.TOP.PERFORMERS.ROUND: {e}")

    # Returnera via write_outputs → korrekt manifest
    payload = {
        "script": f"Driver ran stats sections for {league} on {day}",
        "items": [],
    }

    manifest = utils.write_outputs(
        section_code="S.STATS.DRIVER",
        day=day,
        league=league,
        payload=payload,
        lang=lang,
        status="success",
    )

    print(f"[S.STATS.DRIVER] ✅ Returnerar manifest: {manifest.keys()}")
    return manifest
