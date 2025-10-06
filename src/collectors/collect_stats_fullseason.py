import os
import yaml
from datetime import datetime
from src.collectors.collect_stats import collect_stats
from src.collectors.utils import upload_json_debug


# ------------------------------------------------------
# 🔧 Hjälpfunktioner
# ------------------------------------------------------

def normalize_date(s):
    """Försök tolka olika datumformat och returnera YYYY-MM-DD."""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def extract_match_list(matches):
    """
    Returnera en homogen lista med match-objekt oavsett hur SoccerDataAPI strukturerar svaret.
    Hanterar list/dict med ev. stage-nivåer.
    """
    out = []

    # Om matches är en lista med ligor
    if isinstance(matches, list):
        for league in matches:
            if not isinstance(league, dict):
                continue
            # Fall 1: top-level 'matches'
            if "matches" in league and isinstance(league["matches"], list):
                out.extend(league["matches"])
            # Fall 2: nested 'stage' -> 'matches'
            if "stage" in league and isinstance(league["stage"], list):
                for st in league["stage"]:
                    if isinstance(st, dict) and "matches" in st:
                        out.extend(st["matches"])

    # Om matches är en dict
    elif isinstance(matches, dict):
        if "matches" in matches and isinstance(matches["matches"], list):
            out.extend(matches["matches"])
        if "stage" in matches and isinstance(matches["stage"], list):
            for st in matches["stage"]:
                if isinstance(st, dict) and "matches" in st:
                    out.extend(st["matches"])

    return out


def get_latest_finished(matches):
    """Returnera senaste spelade datum ur matchlistan."""
    match_list = extract_match_list(matches)
    all_dates = []
    for m in match_list:
        if str(m.get("status", "")).lower() == "finished" and m.get("date"):
            all_dates.append(normalize_date(m["date"]))
    if not all_dates:
        return None
    return sorted(all_dates)[-1]


def save_latest_round(league_id, season, matches):
    """Filtrera och ladda upp senaste omgången."""
    match_list = extract_match_list(matches)
    latest_date = get_latest_finished(matches)

    if not latest_date:
        print(f"[collect_stats_fullseason] ⚠️ Inga färdiga matcher hittades för {league_id}")
        return

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


# ------------------------------------------------------
# 🚀 Main
# ------------------------------------------------------

def main():
    season = os.getenv("SEASON", "2025-2026")
    print(f"Running stats for season={season}")

    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for league in config["leagues"]:
        if not league.get("enabled"):
            continue

        league_id = league["id"]
        league_name = league.get("name", f"league_{league_id}")

        print(f"[collect_stats_fullseason] Fetching full season for {league_name} (id={league_id})")

        # 1️⃣ Hämta hela säsongen → matches.json i Azure
        matches = collect_stats(league_id, season, mode="fullseason")

        # 2️⃣ Ladda upp senaste omgången (automatiskt filtrerad)
        save_latest_round(league_id, season, matches)


if __name__ == "__main__":
    main()
