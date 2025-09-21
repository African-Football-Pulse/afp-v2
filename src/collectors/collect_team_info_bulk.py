import os
import requests
import yaml
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")
API_BASE = "https://api.soccerdataapi.com"
AUTH_KEY = os.environ.get("SOCCERDATA_AUTH_KEY")


def load_leagues():
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return [l for l in cfg.get("leagues", []) if l.get("enabled", False)]


def get_active_season(container: str, league_id: int) -> str | None:
    """Läs meta/seasons_{league_id}.json och returnera year för aktiv säsong."""
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


def load_matches(manifest):
    """Lyft från 05: hantera både liga- och cupformat."""
    matches = []
    if isinstance(manifest, dict):
        if "results" in manifest:
            results = manifest["results"]
            if isinstance(results, dict) and "stage" in results:
                for stage in results["stage"]:
                    matches.extend(stage.get("matches", []))
            elif isinstance(results, list):
                for league_data in results:
                    for stage in league_data.get("stage", []):
                        matches.extend(stage.get("matches", []))
    elif isinstance(manifest, list):
        for league_data in manifest:
            for stage in league_data.get("stage", []):
                matches.extend(stage.get("matches", []))
    return matches


def fetch_team_info(team_id: int):
    url = f"{API_BASE}/team/{team_id}"
    headers = {"Authorization": f"Bearer {AUTH_KEY}"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def collect_team_info(container: str, league_id: int, season: str):
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    try:
        manifest = azure_blob.get_json(container, manifest_path)
    except Exception:
        print(f"[collect_team_info_bulk] ⚠️ No manifest found for league {league_id}, season {season}", flush=True)
        return 0

    matches = load_matches(manifest)
    if not matches:
        print(f"[collect_team_info_bulk] ⚠️ No matches found in manifest for league {league_id}", flush=True)
        return 0

    team_ids = set()
    for m in matches:
        teams = m.get("teams", {})
        if "home" in teams:
            team_ids.add(teams["home"]["id"])
        if "away" in teams:
            team_ids.add(teams["away"]["id"])

    if not team_ids:
        print(f"[collect_team_info_bulk] ⚠️ No teams extracted for league {league_id}", flush=True)
        return 0

    out_prefix = f"teams/{league_id}/"
    processed = 0

    for tid in team_ids:
        try:
            data = fetch_team_info(tid)
            azure_blob.upload_json(container, f"{out_prefix}{tid}.json", data)
            processed += 1
        except Exception as e:
            print(f"[collect_team_info_bulk] ⚠️ Could not fetch team {tid}: {e}", flush=True)

    azure_blob.upload_json(container, f"{out_prefix}manifest.json", list(team_ids))
    print(f"[collect_team_info_bulk] Uploaded {processed} teams for league {league_id}, season {season}", flush=True)
    return processed


def main():
    leagues = load_leagues()
    container = CONTAINER
    total = 0

    print("[collect_team_info_bulk] Starting team info collection...", flush=True)

    for league in leagues:
        league_id = league["id"]
        season = get_active_season(container, league_id)
        if not season:
            print(f"[collect_team_info_bulk] ⚠️ No active season found for league {league_id}", flush=True)
            continue
        print(f"[collect_team_info_bulk] league_id={league_id}, active season={season}", flush=True)
        n = collect_team_info(container, league_id, season)
        total += n

    print(f"[collect_team_info_bulk] DONE. Total teams processed: {total}", flush=True)


if __name__ == "__main__":
    main()
