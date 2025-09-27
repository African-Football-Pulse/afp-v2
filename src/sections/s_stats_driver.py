import os
import importlib
from src.sections import utils


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.DRIVER")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    manifest = {"script": f"Stats driver executed for {league} on {day}"}

    # Lista på subsektioner vi vill köra
    subsections = [
        ("S.STATS.TOP.CONTRIBUTORS.SEASON", "s_stats_top_contributors_season"),
        ("S.STATS.TOP.PERFORMERS.ROUND", "s_stats_top_performers_round"),
        ("S.STATS.PROJECT.STATUS", "s_stats_project_status"),
        ("S.STATS.DISCIPLINE", "s_stats_discipline"),
        ("S.STATS.GOAL.IMPACT", "s_stats_goal_impact"),
    ]

    results = []
    for sub_code, module_name in subsections:
        try:
            print(f"[{section_code}] ▶️ Importerar src.sections.{module_name} för {sub_code}")
            mod = importlib.import_module(f"src.sections.{module_name}")

            # Viktigt: injicera rätt section för undersektionen
            setattr(args, "section", sub_code)

            sub_kwargs = {"season": "2025-2026", "league_id": league, "round_dates": []}
            print(f"[{section_code}] ▶️ Kör {sub_code} med kwargs={sub_kwargs}")
            result = mod.build_section(args, **sub_kwargs)

            print(
                f"[{section_code}] ✅ {sub_code} returnerade {type(result)} "
                f"med nycklar: {list(result.keys()) if isinstance(result, dict) else 'N/A'}"
            )
            results.append(result)
        except Exception as e:
            print(f"[{section_code}] ❌ Fel i {sub_code}: {e}")

    # Returnera driverns eget manifest (översikt)
    return utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        lang=lang,
        pod=pod,
        manifest=manifest,
        status="success",
        payload={"subsections": [r for r in results if isinstance(r, dict)]},
    )
