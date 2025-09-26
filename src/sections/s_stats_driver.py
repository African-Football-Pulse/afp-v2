# src/sections/s_stats_driver.py
import os
from src.producer import stats_utils
from src.sections import (
    s_stats_top_performers_round,
    s_stats_project_status,
    s_stats_top_contributors_season,
)

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

# Alla stats-sektioner vi vill köra
STATS_SECTIONS = [
    {"id": "S.STATS.TOP_PERFORMERS_ROUND", "module": s_stats_top_performers_round},
    {"id": "S.STATS.PROJECT_STATUS", "module": s_stats_project_status},
    {"id": "S.STATS.TOP_CONTRIBUTORS_SEASON", "module": s_stats_top_contributors_season},
]


def build_section(args=None):
    """
    Driver för alla STATS-sektioner.
    Loopar igenom ligor i stats/{season}/, hittar nya rundor
    och anropar alla definierade stats-sektioner.
    """
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    outdir = getattr(args, "outdir", "sections")
    section_code = getattr(args, "section", "S.STATS.DRIVER")

    prefix = f"stats/{season}/"
    blobs = stats_utils.azure_blob.list_prefix(CONTAINER, prefix)
    league_ids = sorted({b.split("/")[2] for b in blobs if len(b.split("/")) >= 3})

    state = stats_utils.load_last_stats()
    ran_sections, failed_sections = [], []

    for league_id in league_ids:
        round_dates = stats_utils.find_next_round(season, int(league_id))
        if not round_dates:
            continue

        for section in STATS_SECTIONS:
            try:
                print(f"[s_stats_driver] Kör {section['id']} för liga {league_id}, rundor {round_dates}")
                res = section["module"].build_section(
                    season=season,
                    league_id=int(league_id),
                    round_dates=round_dates,
                    output_prefix=f"{outdir}/{section['id']}/{season}/{league_id}",
                )
                if res:
                    ran_sections.append(section["id"])
            except Exception as e:
                print(f"[s_stats_driver] ❌ Fel i {section['id']} för liga {league_id}: {e}")
                failed_sections.append(section["id"])

        # Uppdatera state när alla sektioner för ligan är körda
        state[str(league_id)] = round_dates[-1]

    if ran_sections:
        stats_utils.save_last_stats(state)

    # Returnera manifest
    return {
        "section": section_code,
        "season": season,
        "league_count": len(league_ids),
        "ran_sections": ran_sections,
        "failed_sections": failed_sections,
        "status": "ok" if ran_sections else "no_data",
    }
