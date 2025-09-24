# src/sections/s_stats_project_status.py

import os
from src.sections import utils as section_utils

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")


def build_section(args=None):
    """
    Statisk status-sektion som berättar om antalet spelare i masterlistan
    och att fler är på väg att läggas till.
    """
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", "unknown")

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
        "league": league,
        "date": day,
        "title": title,
        "status": "ok",
    }

    # ✅ Returnera manifest via write_outputs
    return section_utils.write_outputs(
        section="S.STATS.PROJECT_STATUS",
        season=season,
        league=league,
        round_dates=[],
        output_prefix=f"sections/S.STATS.PROJECT_STATUS/{day}/{league}/_",
        text=text,
        manifest=manifest,
    )
