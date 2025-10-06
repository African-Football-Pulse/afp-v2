import os
import yaml
from datetime import datetime
from src.collectors.collect_stats import collect_stats
from src.collectors.utils import upload_json_debug


def normalize_date(s):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def get_latest_finished(matches):
    """Returnera senaste spelade datum ur matchlistan."""
    # Hantera både list och dict-format
    match_list = matches
    if isinstance(matches, dict):
        match_list = matches.get("matches", [])

    all_dates = []
    for m in match_list:
        if m.get("status") == "finished" and m.get("date"):
            all_dates.append(normalize_date(m["date"]))
    if not all_dates:
        return None
    return sorted(all_dates)[-1]


def save_latest_round(league_id, season, matches):
    """Filtrera och ladda upp senaste omgången."""
    latest_date = get_latest_finished(matches)
    if not latest_date:
        print(f"[collect_stats_fullseason] ⚠️ Inga färdiga matcher hittades för {league_id}")
        return

    match_list = matches
    if isinstance(matches, dict):
        match_list = matches.get("matches", [])

    latest_matches = [
        m for m in match_list if normalize_date(m.get("date")) == latest_date
    ]

    if not latest_matches:
        print(f"[collect_stats_fullseason] ⚠️ Hittade inga matcher för {latest_date} i liga {league_id}")
        return

    out_path = f"stats/{season}/{league_id}/{latest_date}/matches.json"
    try:
        upload_json_debug(out_path, latest_matches)
        print(f"[collect_stats_fullseason] ✅ Uploaded {len(latest_matches)} matches → {out_path}")
    except Exception as e:
        print(f"[collect_stats_fullseason] ⚠️ Misslyckades att ladda upp latest round: {e}")


def main():
    season = os.getenv("SEASON", "2025-2026")
    print(f"Running stats for season={season}")

    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for league in config["leagues"]:
        if not league.get("enabled"):
            continue

        league_id = league["id"]
        print(f"[collect_stats_fullseason] Fetching full season for {league['name']} (id={league_id})")

        matches = collect_stats(league_id, season, mode="fullseason")
        save_latest_round(league_id, season, matches)


if __name__ == "__main__":
    main()
