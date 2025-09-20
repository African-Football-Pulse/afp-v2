import os
import argparse
from src.storage import azure_blob
import yaml

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
MASTER_PATH = "players/africa/players_africa_master.json"
LEAGUES_PATH = "config/leagues.yaml"


def load_master_ids():
    data = azure_blob.get_json(CONTAINER, MASTER_PATH)
    ids = set()
    if isinstance(data, dict) and "players" in data:
        players = data["players"]
    else:
        players = data
    for p in players:
        pid = str(p.get("id", ""))
        if pid.isdigit():
            ids.add(pid)
    return ids


def load_leagues():
    with open(LEAGUES_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return [l for l in config.get("leagues", []) if l.get("enabled", False)]


def load_manifest(season: str, league_id: str):
    path = f"stats/{season}/{league_id}/manifest.json"
    try:
        return azure_blob.get_json(CONTAINER, path)
    except Exception as e:
        print(f"[WARN] Could not load manifest {path}: {e}")
        return None


def collect_player_history(league_id: str, season: str, africa_ids: set):
    manifest = load_manifest(season, league_id)
    if not manifest or not isinstance(manifest, list):
        return {}

    player_history = {}

    for stage in manifest[0].get("stage", []):
        for match in stage.get("matches", []):
            home_team = match.get("teams", {}).get("home", {})
            away_team = match.get("teams", {}).get("away", {})

            for ev in match.get("events", []):
                for role in ["player", "player_in", "player_out", "assist_player"]:
                    p = ev.get(role)
                    if p and "id" in p:
                        pid = str(p["id"])
                        pname = p.get("name", "Unknown")

                        if pid not in africa_ids:
                            continue

                        if pid not in player_history:
                            player_history[pid] = {
                                "id": pid,
                                "name": pname,
                                "history": []
                            }

                        team_side = ev.get("team")
                        team_info = home_team if team_side == "home" else away_team if team_side == "away" else {}

                        entry = {
                            "league_id": league_id,
                            "season": season,
                            "club_id": team_info.get("id"),
                            "club_name": team_info.get("name")
                        }

                        if entry not in player_history[pid]["history"]:
                            player_history[pid]["history"].append(entry)

    return player_history


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season (e.g., 2024-2025)")
    args = parser.parse_args()

    africa_ids = load_master_ids()
    leagues = load_leagues()

    for league in leagues:
        league_id = str(league["id"])
        history = collect_player_history(league_id, args.season, africa_ids)

        if not history:
            print(f"[collect_player_history_bulk] No data for league {league_id}")
            continue

        out_path = f"meta/{args.season}/player_history_{league_id}.json"
        azure_blob.upload_json(CONTAINER, out_path, history)
        print(f"[collect_player_history_bulk] Uploaded → {out_path} ({len(history)} players)")


if __name__ == "__main__":
    main()
