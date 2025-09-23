# src/sections/s_stats_top_performers_round.py

import os
from collections import defaultdict
from src.storage import azure_blob
from src.producer import stats_utils
from src.sections import utils as section_utils

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def build_section(args):
    """
    PRODUCE entrypoint: kör Top Performers för en hel omgång.
    args: argparse.Namespace från produce_section.py
    """
    league = getattr(args, "league", "premier_league")
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    output_prefix = getattr(args, "outdir", "sections")

    # TODO: just nu hårdkodat till dagens datum – kan göras smartare
    round_dates = [getattr(args, "date", os.getenv("DATE", ""))]

    # Hämta events och spara fil i Azure
    blob_path = stats_utils.save_african_events(
        season=season, league_id=league, round_dates=round_dates, scope="round"
    )
    if not blob_path:
        return None

    events = azure_blob.get_json(CONTAINER, blob_path)
    if not events:
        return None

    # Summera statistik
    performers = defaultdict(lambda: {"goals": 0, "assists": 0, "cards": 0, "name": ""})
    for ev in events:
        pid = ev["player"]["id"]
        pname = ev["player"]["name"]
        performers[pid]["name"] = pname
        if ev["event_type"] == "goal":
            performers[pid]["goals"] += 1
        elif ev["event_type"] == "assist":
            performers[pid]["assists"] += 1
        elif ev["event_type"] in ["yellow_card", "red_card"]:
            performers[pid]["cards"] += 1

    if not performers:
        return None

    # Sortera topp 3
    top_players = sorted(
        performers.values(),
        key=lambda x: (x["goals"] + x["assists"]),
        reverse=True
    )[:3]

    # Bygg text
    lines = [
        f"- {p['name']} ({p['goals']} mål, {p['assists']} assist, {p['cards']} kort)"
        for p in top_players
    ]
    section_text = "Helgens afrikanska topp-prestationer:\n" + "\n".join(lines)

    manifest = {
        "season": season,
        "league": league,
        "round_dates": round_dates,
        "count": len(top_players),
        "players": [p["name"] for p in top_players],
    }

    return section_utils.write_outputs(
        container=CONTAINER,
        prefix=output_prefix,
        section_id="S.STATS.TOP_PERFORMERS_ROUND",
        text=section_text,
        manifest=manifest,
    )
