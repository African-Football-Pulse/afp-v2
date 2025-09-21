import os
import argparse
import json
import yaml
from src.storage import azure_blob


MASTER_PATH = "players/africa/players_africa_master.json"
LEAGUES_PATH = "config/leagues.yaml"


def load_master_ids(container: str):
    data = azure_blob.get_json(container, MASTER_PATH)
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


def load_matches(container: str, season: str, league: dict):
    league_id = league["id"]
    is_cup = league.get("is_cup", False)
    manifest_path = f"stats/{season}/{league_id}/manifest.json"

    try:
        manifest_text = azure_blob.get_text(container, manifest_path)
    except Exception:
        print(f"[player_history_bulk] ⚠️ No manifest found for league_id={league_id}", flush=True)
        return []

    try:
        manifest = json.loads(manifest_text)
    except Exception as e:
        print(f"[player_history_bulk] ⚠️ Could not parse manifest for league_id={league_id} ({e})", flush=True)
        return []

    matches = []

    if is_cup:
        if isinstance(manifest, dict) and isinstance(manifest.get("results"), list):
            for stage in manifest.get("results", []):
                matches.extend(stage.get("matches", []))
        elif isinstance(manifest, list):
            for league_data in manifest:
                for stage in league_data.get("stage", []):
                    matches.extend(stage.get("matches", []))
    else:
        if isinstance(manifest, dict) and isinstance(manifest.get("results"), list):
            matches = manifest["results"]
        elif isinstance(manifest, list):
            for league_data in manifest:
                for stage in league_data.get("stage", []):
                    matches.extend(stage.get("matches", []))

    print(f"[player_history_bulk] league_id={league_id}: {len(matches)} matches found", flush=True)
    return matches


def build_player_history(matches: list, league_id: str, season: str, africa_ids: set):
    player_history = {}

    for match in matches:
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

    # ✅ hämta container med guard mot None/tom sträng
    container = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
    africa_ids = load_master_ids(container)
    leagues = load_leagues()

    print(f"[player_history_bulk] Starting player history collection for season {args.season}", flush=True)

    total_players = 0
    for league in leagues:
        league_id = str(league["id"])
        matches = load_matches(container, args.season, league)
        if not matches:
            continue

        history = build_player_history(matches, league_id, args.season, africa_ids)
        if not history:
            print(f"[player_history_bulk] No African players found for league {league_id}", flush=True)
            continue

        out_path = f"meta/{args.season}/player_history_{league_id}.json"
        azure_blob.upload_json(container, out_path, history)
        print(f"[player_history_bulk] Uploaded → {out_path} ({len(history)} players)", flush=True)

        total_players += len(history)

    print(f"[player_history_bulk] DONE. Total players with history this season: {total_players}", flush=True)


if __name__ == "__main__":
    main()
