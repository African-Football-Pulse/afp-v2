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

                # Kolla målskytt/assist
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


# ---------- Ny state-hantering ----------

STATE_PATH = "sections/state/last_stats.json"


def load_last_stats() -> Dict[str, str]:
    """
    Hämta senaste körda stats-datum per liga från Azure.
    Returnerar dict: { league_id: last_date }
    """
    try:
        return azure_blob.get_json(CONTAINER, STATE_PATH)
    except Exception:
        return {}


def save_last_stats(state: Dict[str, str]):
    """
    Spara senaste stats-datum per liga till Azure.
    """
    azure_blob.upload_json(CONTAINER, STATE_PATH, state)


def list_available_rounds(season: str, league_id: int) -> List[str]:
    """
    Lista alla datum-mappar som finns i stats/{season}/{league_id}/ i Azure.
    Returnerar sorterad lista [ "20-09-2025", "21-09-2025", ... ]
    """
    prefix = f"stats/{season}/{league_id}/"
    blobs = azure_blob.list_prefix(CONTAINER, prefix)
    rounds = set()
    for b in blobs:
        parts = b.split("/")
        if len(parts) >= 4:
            rounds.add(parts[3])  # stats/<season>/<league_id>/<round_date>/...
    return sorted(rounds)


def find_next_round(season: str, league_id: int) -> List[str]:
    """
    Hitta nästa runda (en eller flera datum) som inte körts än.
    Returnerar listan av datum (ex: ["20-09-2025","21-09-2025"]) eller [] om inget nytt.
    """
    last_state = load_last_stats()
    last_date = last_state.get(str(league_id))

    available = list_available_rounds(season, league_id)
    if not available:
        return []

    # Om ingen state finns → ta senaste runda (alla datum samma helg)
    if not last_date:
        return [available[-1]]

    # Hitta om det finns datum nyare än last_date
    if last_date in available:
        idx = available.index(last_date)
        if idx + 1 < len(available):
            return [available[idx + 1]]
        else:
            return []
    else:
        # last_date ej i listan → ta sista som fallback
        return [available[-1]]
