# src/sections/s_stats_project_status.py
import os
from src.sections import utils


def build_section(args=None, **kwargs):
    """
    Static status section about project progress.
    Kompatibel med både standalone och körning via s_stats_driver.
    """
    season = kwargs.get("season", getattr(args, "season", os.getenv("SEASON", "2025-2026")))
    league = kwargs.get("league_id", getattr(args, "league", os.getenv("LEAGUE", "premier_league")))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    section_code = getattr(args, "section", "S.STATS.PROJECT.STATUS")

    
    

    title = "Project Status"
    text = (
        "We are currently tracking **51 African players** in the Premier League. "
        "In the coming weeks, we will add around **30 more players**, "
        "bringing our coverage to roughly 80 players. "
        "And the journey doesn’t stop there – we will keep adding more names continuously. "
        "Stay tuned as the lists grow and become even more complete!"
    )

    payload = {
        "slug": "project_status",
        "title": title,
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {},
        "meta": {"persona": "storyteller"},
        "type": "stats",
        "model": "static",
    }

    return utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        payload=payload,
        lang=lang,
        status="ok",
    )
