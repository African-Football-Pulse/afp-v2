import os
from collections import defaultdict
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")


def load_master(container: str):
    path = "players/africa/players_africa_master.json"
    master = azure_blob.get_json(container, path)
    if not master or "players" not in master:
        raise RuntimeError(f"[collect_teams_current_bulk] Missing or invalid master file at {path}")
    return master["players"]


def collect_current_teams(container: str, season: str):
    players = load_master(container)
    teams = defaultdict(lambda: {"club_name": None, "players": []})

    for p in players:
        club = p.get("club")
        club_id = p.get("club_id")  # vi bör ha detta i master, annars blir det problem
        if not club_id or not club:
            continue

        if teams[club_id]["club_name"] is None:
            teams[club_id]["club_name"] = club

        teams[club_id]["players"].append({
            "id": str(p.get("id")),
            "name": p.get("name"),
            "country": p.get("country")
        })

    out_path = f"teams/current/{season}.json"
    azure_blob.upload_json(container, out_path, teams)
    print(f"[collect_teams_current_bulk] Uploaded → {out_path}")
    print(f"[collect_teams_current_bulk] Total teams with African players: {len(teams)}")


def main():
    # Pågående säsong hämtas från ligorna – men vi kan hårdkoda här
    # eftersom den gäller alla ligor samma år (ex: 2025-2026).
    season = "2025-2026"

    print("[collect_teams_current_bulk] Starting current season team build...", flush=True)
    collect_current_teams(CONTAINER, season)
    print("[collect_teams_current_bulk] DONE.", flush=True)


if __name__ == "__main__":
    main()
