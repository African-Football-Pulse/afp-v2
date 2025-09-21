import os
import yaml
from collections import defaultdict
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")


def load_leagues():
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return [l for l in cfg.get("leagues", []) if l.get("enabled", False) and not l.get("is_cup", False)]


def get_active_season(container: str, league_id: int) -> str | None:
    """Hämta pågående säsong från meta/seasons_{league_id}.json."""
    try:
        data = azure_blob.get_json(container, f"meta/seasons_{league_id}.json")
    except Exception:
        return None

    results = data.get("results", [])
    for entry in results:
        season = entry.get("season", {})
        if season.get("is_active"):
            return season.get("year")
    return None


def load_master(container: str):
    path = "players/africa/players_africa_master.json"
    master = azure_blob.get_json(container, path)
    if not master or "players" not in master:
        raise RuntimeError(f"[collect_teams_current_bulk] Missing or invalid master file at {path}")
    return master["players"]


def load_team_info(container: str, league_id: int, team_id: int):
    path = f"teams/{league_id}/{team_id}.json"
    try:
        return azure_blob.get_json(container, path)
    except Exception:
        return None


def collect_current_teams(container: str, league_id: int, season: str):
    players = load_master(container)
    teams = defaultdict(lambda: {"club_name": None, "players": []})

    for p in players:
        club_id = p.get("club_id")
        if not club_id:
            continue

        # ladda klubbinfo
        if teams[club_id]["club_name"] is None:
            team_info = load_team_info(container, league_id, club_id)
            if team_info:
                teams[club_id]["club_name"] = team_info.get("name")

        teams[club_id]["players"].append({
            "id": str(p.get("id")),
            "name": p.get("name"),
            "country": p.get("country")
        })

    out_path = f"meta/{season}/teams_{league_id}.json"
    azure_blob.upload_json(container, out_path, teams)
    print(f"[collect_teams_current_bulk] Uploaded → {out_path}")
    print(f"[collect_teams_current_bulk] Total teams with African players: {len(teams)}")


def main():
    leagues = load_leagues()
    container = CONTAINER

    print("[collect_teams_current_bulk] Starting current season team build...", flush=True)

    for league in leagues:
        league_id = league["id"]
        season = get_active_season(container, league_id)
        if not season:
            print(f"[collect_teams_current_bulk] ⚠️ No active season found for league {league_id}", flush=True)
            continue

        print(f"[collect_teams_current_bulk] league_id={league_id}, active season={season}", flush=True)
        collect_current_teams(container, league_id, season)

    print("[collect_teams_current_bulk] DONE.", flush=True)


if __name__ == "__main__":
    main()
