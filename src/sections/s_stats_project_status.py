# src/sections/s_stats_project_status.py

import os
from src.sections import utils as section_utils

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def build_section(season: str, league_id: int, round_dates: list, output_prefix: str):
    """
    Bygger en statisk status-sektion som berättar om omfattningen
    av våra afrikanska spelare i masterlistan och att vi expanderar.
    """

    title = "Status på vårt projekt"
    text = (
        "Vi följer just nu **51 afrikanska spelare** i Premier League. "
        "Under de kommande veckorna kompletterar vi med ytterligare omkring "
        "**30 spelare**, vilket gör att vår bevakning snart omfattar cirka 80 spelare. "
        "Och resan slutar inte där – vi fortsätter att lägga till fler namn löpande. "
        "Häng med när listorna växer och blir allt mer kompletta!"
    )

    manifest = {
        "section": "S.STATS.PROJECT_STATUS",
        "season": season,
        "league_id": league_id,
        "round_dates": round_dates,
        "title": title,
    }

    return section_utils.write_outputs(
        section="S.STATS.PROJECT_STATUS",
        season=season,
        league_id=league_id,
        round_dates=round_dates,
        output_prefix=output_prefix,
        text=text,
        manifest=manifest,
    )
