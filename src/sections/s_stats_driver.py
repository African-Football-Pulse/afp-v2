# src/sections/s_stats_driver.py
import os
import importlib
from src.sections import utils

def build_section(args, **kwargs):
    """Driver som kör alla stats-sektioner för en given dag/league."""

    # Extract arguments with fallbacks
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    section_code = getattr(args, "section", "S.STATS.DRIVER")

    print(f"[{section_code}] 🚀 Startar driver för stats (league={league}, day={day})")

    # Lista på subsektioner och deras moduler
    subsections = {
        "S.STATS.TOP.CONTRIBUTORS.SEASON": "src.sections.s_stats_top_contributors_season",
        "S.STATS.TOP.PERFORMERS.ROUND": "src.sections.s_stats_top_performers_round",
        "S.STATS.PROJECT.STATUS": "src.sections.s_stats_project_status",
    }

    results = {}
    for sub_code, module_path in subsections.items():
        try:
            print(f"[{section_code}] ▶️ Kör {sub_code} via {module_path}")
            mod = importlib.import_module(module_path)

            # Bygg kwargs för undersektionen
            sub_kwargs = {
                "season": season,
                "league_id": league,
                "round_dates": kwargs.get("round_dates", []),
            }

            # Kör build_section i undersektionen
            result = mod.build_section(args, **sub_kwargs)
            results[sub_code] = {"status": "done", "result": result}
        except Exception as e:
            print(f"[{section_code}] ❌ Fel i {sub_code}: {e}")
            results[sub_code] = {"status": "error", "error": str(e)}

    # Manifest & payload för drivern
    text = f"Stats driver ran {len(subsections)} subsections for {league} on {day}"
    payload = {
        "slug": "stats_driver",
        "title": "Stats Driver",
        "text": text,
        "meta": {"league": league, "day": day, "subsections": list(subsections.keys())},
        "type": "stats",
        "model": "driver",
        "items": list(results.keys()),
    }
    manifest = {"script": text, "meta": {"subsections": list(subsections.keys())}}

    result = utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        lang=lang,
        pod=pod,
        manifest=manifest,
        status="success",
        payload=payload,
    )

    print(f"[{section_code}] ✅ Driver klar – returnerar manifest")
    return result
