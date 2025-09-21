import os
import argparse
import json
from src.storage import azure_blob
import yaml

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
MASTER_PATH = "players/africa/players_africa_master.json"


def load_master_ids(container: str):
    master = azure_blob.get_json(container, MASTER_PATH)
    return set(master.keys()), master


def load_leagues():
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return [l for l in cfg.get("leagues", []) if l.get("enabled", False)]


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
            for stage in manifest["results"]:
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

    return matches


def collect_player_history(container: str, season: str, master_ids: set, master: dict):
    leagues = load_leagues()
    players_history = {}

    for league in leagues:
        league_id = league["id"]
        matches = load_matches(container, season, league)
        if not matches:
            continue

        for m in matches:
            for side in ["home_team", "away_team"]:
                team = m.get(side) or m.get("teams", {}).get("home" if side == "home_team" else "away", {})
                team_id = team.get("id")
                players = m.get(f"{side}_players", [])

                for p in players:
                    pid = str(p.get("id"))
                    if pid not in master_ids:
                        continue

                    entry = {
                        "season": season,
                        "league_id": league_id,
                        "team_id": team_id
                    }

                    players_history.setdefault(pid, {"history": []})
                    # undvik duplicat (samma säsong/lag flera gånger)
                    if entry not in players_history[pid]["history"]:
                        players_history[pid]["history"].append(entry)

    # ladda upp en fil per liga
    out_path = f"meta/{season}/player_history_{league_id}.json"
    azure_blob.upload_json(container, out_path, players_history)
    print(f"[player_history_bulk] Uploaded → {out_path} ({len(players_history)} players)", flush=True)

    return players_history


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season, e.g. 2024-2025")
    args = parser.parse_args()

    container = CONTAINER
    season = args.season
    master_ids, master = load_master_ids(container)

    print(f"[player_history_bulk] Starting player history collection for season {season}", flush=True)

    history = collect_player_history(container, season, master_ids, master)

    print(f"[player_history_bulk] DONE. Total players with history this season: {len(history)}", flush=True)


if __name__ == "__main__":
    main()
