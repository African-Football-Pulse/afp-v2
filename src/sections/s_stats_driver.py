# src/sections/s_stats_driver.py
import os
from src.sections import utils

def build_section(args, **kwargs):
    """Driver som kör alla stats-sektioner för en given dag/league."""

    # Extract arguments with fallbacks
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")
    section_code = getattr(args, "section", "S.STATS.DRIVER")

    print(f"[{section_code}] 🚀 Startar driver för stats (league={league}, day={day})")

    # Här ska riktiga anrop till undersektioner in (exempel)
    subsections = [
        "S.STATS.TOP.CONTRIBUTORS.SEASON",
        "S.STATS.TOP.PERFORMERS.ROUND",
    ]
    for sub in subsections:
        try:
            print(f"[{section_code}] Kör {sub}")
            # TODO: Importera modul och kör dess build_section(...)
        except Exception as e:
            print(f"[{section_code}] ❌ Fel i {sub}: {e}")

    # Manifest & payload för drivern
    text = f"Stats driver ran subsections for {league} on {day}"
    payload = {
        "slug": "stats_driver",
        "title": "Stats Driver",
        "text": text,
        "meta": {"league": league, "day": day, "subsections": subsections},
        "type": "stats",
        "model": "driver",
        "items": [],
    }
    manifest = {"script": text, "meta": {"subsections": subsections}}

    result = utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        lang=lang,
        pod=pod,
        manifest=ma
