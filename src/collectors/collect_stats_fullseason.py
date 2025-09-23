import os
from src.collectors.collect_stats import collect_stats
from src.collectors.utils import get_latest_finished_date, download_json_debug, upload_json_debug


def save_latest_round(league_id: int, season: str, matches: dict, manifest: dict):
    """
    Filtrera ut senaste omgången ur matches.json och ladda upp den till Azure.
    """
    latest_date = get_latest_finished_date(manifest)
    if not latest_date:
        print(f"[collect_stats_fullseason] ⚠️ Inget giltigt datum hittades för {league_id}")
        return

    latest_matches = []
    for league in matches if isinstance(matches, list) else [matches]:
        for m in league.get("matches", []):
            if m.get("date") == latest_date:
                latest_matches.append(m)

    if not latest_matches:
        print(f"[collect_stats_fullseason] ⚠️ Hittade inga matcher för {latest_date} i liga {league_id}")
        return

    safe_date = latest_date.replace("/", "-")
    out_path = f"stats/{season}/{league_id}/{safe_date}/matches.json"

    try:
        upload_json_debug(out_path, latest_matches)
        print(f"[collect_stats_fullseason] ✅ Uploaded {len(latest_matches)} matches for {league_id} on {latest_date} → {out_path}")
    except Exception as e:
        print(f"[collect_stats_fullseason] ⚠️ Misslyckades att ladda upp latest round för {league_id}: {e}")


def main():
    season = os.getenv("SEASON", "2025-2026")
    print(f"Running stats for season={season}")

    import yaml
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for league in config["leagues"]:
        if not league.get("enabled"):
            continue

        league_id = league["id"]
        print(f"[collect_stats_fullseason] Fetching full season for {league['name']} (id={league_id}, season={season})")

        # 1. Hämta hela säsongen → matches.json i Azure
        matches = collect_stats(league_id, season, mode="fullseason")

        # 2. Ladda manifestet från Azure
        manifest_path = f"stats/{season}/{league_id}/manifest.json"
        manifest = download_json_debug(manifest_path)

        if not manifest:
            print(f"[collect_stats_fullseason] ⚠️ Ingen manifest för {league_id}, hoppar över latest round-save.")
            continue

        # 3. Filtrera & ladda upp senaste omgången
        save_latest_round(league_id, season, matches, manifest)


if __name__ == "__main__":
    main()
