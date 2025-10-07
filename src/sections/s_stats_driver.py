import os
import importlib
import logging
from src.sections import utils

# -------------------------------------------------------
# Logger setup (driver)
# -------------------------------------------------------
logger = logging.getLogger("driver")
handler = logging.StreamHandler()
formatter = logging.Formatter("[driver] %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]
logger.propagate = False
logger.setLevel(logging.INFO)


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.DRIVER")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    # 🧩 Dynamiskt fastställ aktuell säsong
    season = utils.current_season()
    logger.info(f"🏆 Running stats driver for {league}, season={season}, day={day}")

    manifest = {"script": f"Stats driver executed for {league} on {day} ({season})"}

    # Lista på subsektioner som ska köras
    subsections = [
        ("S.STATS.TOP.CONTRIBUTORS.SEASON", "s_stats_top_contributors_season"),
        ("S.STATS.TOP.PERFORMERS.ROUND", "s_stats_top_performers_round"),
        ("S.STATS.PROJECT.STATUS", "s_stats_project_status"),
        ("S.STATS.DISCIPLINE", "s_stats_discipline"),
        ("S.STATS.GOAL.IMPACT", "s_stats_goal_impact"),
    ]

    results = []
    for sub_code, module_name in subsections:
        logger.info("▶️ Startar %s", sub_code)
        try:
            # Dynamisk import
            mod = importlib.import_module(f"src.sections.{module_name}")

            # Uppdatera argumentobjekt
            setattr(args, "section", sub_code)

            # 🧩 Passa in säsong & ev round_dates
            sub_kwargs = {
                "season": season,
                "league_id": league,
                "round_dates": [],
            }

            result = mod.build_section(args, **sub_kwargs)

            status = result.get("status", "unknown") if isinstance(result, dict) else "unknown"
            path = result.get("path", "n/a") if isinstance(result, dict) else "n/a"

            logger.info("✅ %s klar – status=%s, path=%s", sub_code, status, path)
            results.append({"section": sub_code, "status": status, "path": path})
        except Exception as e:
            logger.error("❌ Fel i %s: %s", sub_code, e)
            results.append({"section": sub_code, "status": "error", "path": str(e)})

    logger.info("=== Sammanfattning ===")
    for r in results:
        logger.info("%s: %s (%s)", r["section"], r["status"], r["path"])

    # Returnera driverns eget manifest (översikt)
    return utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        lang=lang,
        pod=pod,
        manifest=manifest,
        status="success",
        payload={"season": season, "subsections": results},
    )
