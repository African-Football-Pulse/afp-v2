import os
import argparse
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def load_manifest(season: str, league_id: str):
    path = f"stats/{season}/{league_id}/manifest.json"
    try:
        return azure_blob.get_json(CONTAINER, path)
    except Exception as e:
        print(f"[WARN] Could not load manifest {path}: {e}")
        return None


def collect_player_history(league_id: str, season: str):
    manifest = load_manifest(season, league_id)
    if not manifest or not isinstance(manifest, list):
        return {}

    player_history = {}

    for stage in manifest[0].get("stage", []):
        for match in stage.get("matches", []):
            # home and away teams
            home_team = match.get("teams", {}).get("home", {})
            away_team = match.get("teams", {}).get("away", {})

            for ev in match.get("events", []):
                # check all roles where a player can appear
                for role in ["player", "player_in", "player_out", "assist_player"]:
                    p = ev.get(role)
                    if p and "id" in p:
                        pid = str(p["id"])
                        pname = p.get("name", "Unknown")

                        if pid not in player_history:
                            player_history[pid] = {
                                "id": pid,
                                "name": pname,
                                "history": []
                            }

                        # best guess: assign to home/away team based on event
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
    parser.add_argument("--league-id", required=True, help="League ID (e.g., 228 for PL)")
    parser.add_argument("--season", required=True, help="Season (e.g., 2024-2025)")
    args = parser.parse_args()

    history = collect_player_history(args.league_id, args.season)

    out_path = f"meta/{args.season}/player_history_{args.league_id}.json"
    azure_blob.upload_json(CONTAINER, out_path, history)
    print(f"[collect_player_history] Uploaded → {out_path}")


if __name__ == "__main__":
    main()
