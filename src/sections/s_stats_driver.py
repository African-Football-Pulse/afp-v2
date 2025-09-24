# src/sections/s_stats_driver.py

import os
from src.producer import stats_utils
from src.sections import (
    s_stats_top_performers_round,
    # s_stats_discipline,
    # s_stats_goal_impact,
)

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

# Lista över alla stats-sektioner vi vill köra
STATS_SECTIONS = [
    {
        "id": "S.STATS.TOP_PERFORMERS_ROUND",
        "module": s_stats_top_performers_round,
    },
    # {
    #     "id": "S.STATS.DISCIPLINE",
    #     "module": s_stats_discipline,
    # },
    # {
    #     "id": "S.STATS.GOAL_IMPACT",
    #     "module": s_stats_goal_impact,
    # },
]

def build_section(args=None):
    """
    Driver för alla STATS-sektioner.
    Loopar igenom ligor i stats/{season}/, hittar nya rundor och
    anropar alla definierade stats-sektioner.
    """
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    outdir = getattr(args, "outdir", "sections")

    results = []
    prefix = f"stats/{season}/"
    blobs = stats_utils.azure_blob.list_prefix(CONTAINER, prefix)
    league_ids = sorted({b.split("/")[2] for b in blobs if len(b.split("/")) >= 3})

    # Ladda state
    state = stats_utils.load_last_stats()

    for league_id in league_ids:
        round_dates = stats_utils.find_next_round(season, int(league_id))
        if not round_dates:
            continue

        for section in STATS_SECTIONS:
            res = section["module"].build_section(
                season=season,
                league_id=int(league_id),
                round_dates=round_dates,
                output_prefix=f"{outdir}/{section['id']}/{season}/{league_id}"
            )
            if res:
                results.append(res)

        # Uppdatera state för denna liga efter alla sektioner
        state[str(league_id)] = round_dates[-1]

    if results:
        stats_utils.save_last_stats(state)

    # ✅ Returnera manifest istället för lista
    return {
        "section": "S.STATS.DRIVER",
        "status": "ok" if results else "no_data",
        "season": season,
        "league_count": len(league_ids),
        "subsections": [s["id"] for s in STATS_SECTIONS],
    }
