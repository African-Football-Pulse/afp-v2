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
        print(f"[{section_code}] ▶️ Startar {sub_code}")
        try:
            mod = importlib.import_module(f"src.sections.{module_name}")
            setattr(args, "section", sub_code)
            sub_kwargs = {"season": "2025-2026", "league_id": league, "round_dates": []}
            result = mod.build_section(args, **sub_kwargs)

            status = result.get("status", "unknown") if isinstance(result, dict) else "unknown"
            path = result.get("path", "n/a") if isinstance(result, dict) else "n/a"

            print(f"[{section_code}] ✅ {sub_code} klar – status={status}, path={path}")
            results.append({"section": sub_code, "status": status, "path": path})
        except Exception as e:
            print(f"[{section_code}] ❌ Fel i {sub_code}: {e}")
            results.append({"section": sub_code, "status": "error", "path": str(e)})

    print("=== [driver] Sammanfattning ===")
    for r in results:
        print(f"{r['section']}: {r['status']} ({r['path']})")

    # Returnera driverns eget manifest (översikt)
    return utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        lang=lang,
        pod=pod,
        manifest=manifest,
        status="success",
        payload={"subsections": results},
    )
