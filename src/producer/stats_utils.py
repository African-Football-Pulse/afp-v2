# src/producer/stats_utils.py

import os
import json
from typing import List, Dict, Any
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def load_masterlist() -> Dict[int, Dict[str, Any]]:
    """
    Laddar masterlistan och returnerar en dict med player_id som nyckel.
    """
    blob_path = "players/africa/players_africa_master.json"
    data = azure_blob.get_json(CONTAINER, blob_path)
    players = {}
    for p in data.get("players", []):
        players[p["id"]] = p
    return players


def extract_african_events(season: str, league_id: int, round_dates: List[str]) -> List[Dict[str, Any]]:
    """
    Läser matcher för angivna datum (en hel omgång), extraherar events kopplat till afrikanska spelare.
    Returnerar en lista av african_events.
    """
    master = load_masterlist()
    african_ids = set(master.keys())
    events: List[Dict[str, Any]] = []

    for round_date in round_dates:
        blob_path = f"stats/{season}/{league_id}/{round_date}/matches.json"
        if not azure_blob.exists(CONTAINER, blob_path):
            continue
        matches = azure_blob.get_json(CONTAINER, blob_path)

        for match in matches:
            match_id = match["id"]
            date = match["date"]
            home = match["teams"]["home"]["name"]
            away = match["teams"]["away"]["name"]

            for ev in match.get("events", []):
                player = ev.get("player")
                assist = ev.get("assist_player")

                # Kolla målskytt/annan spelare
                for role, pl in [("main", player), ("assist", assist)]:
                    if not pl:
                        continue
                    pid = pl.get("id")
                    if pid in african_ids:
                        events.append({
                            "date": date,
                            "league_id": league_id,
                            "match_id": match_id,
                            "home_team": home,
                            "away_team": away,
                            "event_type": ev["event_type"] if role == "main" else "assist",
                            "minute": ev.get("event_minute"),
                            "player": {
                                "id": pid,
                                "name": pl.get("name"),
                                "country": master[pid]["country"],
                                "club": master[pid]["club"],
                            },
                            "related_player": player if role == "assist" else assist
                        })
    return events


def save_african_events(season: str, league_id: int, round_dates: List[str], scope: str = "round"):
    """
    Hämtar och sparar african_events till Azure.
    scope = "round" eller "daily" beroende på sammanhang.
    """
    events = extract_african_events(season, league_id, round_dates)
    if not events:
        return None

    blob_path = f"stats/{season}/{league_id}/{'+'.join(round_dates)}/african_events_{scope}.json"
    azure_blob.upload_json(CONTAINER, blob_path, events)
    return blob_path

