# src/sections/s_stats_top_performers_round.py  (läggs här med de andra sektionerna)

import os
from collections import defaultdict
from src.storage import azure_blob
from src.producer import stats_utils
from src.sections import utils as section_utils

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def build_section(season: str, league_id: int, round_dates: list, output_prefix: str):
    """
    Producerar sektionen 'Top Performers' för en hel ligaomgång.
    round_dates = t.ex. ["20-09-2025", "21-09-2025"]
    """
    # Skapa / hämta eventfil (sparas även i Azure)
    blob_path = stats_utils.save_african_events(
        season=season, league_id=league_id, round_dates=round_dates, scope="round"
    )
    if not blob_path:
        return None

    events = azure_blob.get_json(CONTAINER, blob_path)
    if not events:
        return None

    # Summera statistik per spelare
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

    # Sortera topp 3 (mål + assist → mest betydelse)
    top_players = sorted(
        performers.values(),
        key=lambda x: (x["goals"] + x["assists"]),
        reverse=True
    )[:3]

    # Bygg sektionstext
    lines = []
    for p in top_players:
        lines.append(
            f"- {p['name']} ({p['goals']} mål, {p['assists']} assist, {p['cards']} kort)"
        )
    section_text = "Helgens afrikanska topp-prestationer:\n" + "\n".join(lines)

    manifest = {
        "season": season,
        "league_id": league_id,
        "round_dates": round_dates,
        "count": len(top_players),
        "players": [p["name"] for p in top_players],
    }

    # Spara output via sections/utils
    return section_utils.write_outputs(
        container=CONTAINER,
        prefix=output_prefix,
        section_id="S.STATS.TOP_PERFORMERS_ROUND",
        text=section_text,
        manifest=manifest,
    )
