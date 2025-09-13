import os
import json
import argparse
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def load_manifest(season: str, league_id: str):
    path = f"stats/{season}/{league_id}/manifest.json"
    print(f"[collect_player_stats] Loading matches from {path} ...")
    return azure_blob.get_json(CONTAINER, path)


def collect_player_stats(player_id: str, league_id: str, season: str):
    manifest = load_manifest(season, league_id)

    if not isinstance(manifest, list):
        print("[collect_player_stats] Unexpected manifest format")
        return

    # counters
    apps = 0
    goals = 0
    penalty_goals = 0
    assists = 0
    yellow_cards = 0
    red_cards = 0
    subs_in = 0
    subs_out = 0

    player_id_int = int(player_id)

    for match in manifest:
        events = match.get("events", [])
        appeared = False

        for ev in events:
            etype = ev.get("event_type")

            # Goals
            if etype == "goal" and ev.get("player", {}).get("id") == player_id_int:
                goals += 1
                appeared = True
                print(f"[DEBUG] Goal for {player_id} in match {match.get('id')} at minute {ev.get('event_minute')}")

            if etype == "penalty_goal" and ev.get("player", {}).get("id") == player_id_int:
                penalty_goals += 1
                goals += 1
                appeared = True
                print(f"[DEBUG] Penalty goal for {player_id} in match {match.get('id')} at minute {ev.get('event_minute')}")

            # Assists
            if ev.get("assist_player", {}).get("id") == player_id_int:
                assists += 1
                appeared = True
                print(f"[DEBUG] Assist for {player_id} in match {match.get('id')} at minute {ev.get('event_minute')}")

            # Cards
            if etype == "yellow_card" and ev.get("player", {}).get("id") == player_id_int:
                yellow_cards += 1
                appeared = True
                print(f"[DEBUG] Yellow card for {player_id} in match {match.get('id')} at minute {ev.get('event_minute')}")

            if etype == "red_card" and ev.get("player", {}).get("id") == player_id_int:
                red_cards += 1
                appeared = True
                print(f"[DEBUG] Red card for {player_id} in match {match.get('id')} at minute {ev.get('event_minute')}")

            # Substitutions
            if etype == "substitution":
                if ev.get("player_in", {}).get("id") == player_id_int:
                    subs_in += 1
                    appeared = True
                    print(f"[DEBUG] Substitution IN for {player_id} in match {match.get('id')} at minute {ev.get('event_minute')}")

                if ev.get("player_out", {}).get("id") == player_id_int:
                    subs_out += 1
                    appeared = True
                    print(f"[DEBUG] Substitution OUT for {player_id} in match {match.get('id')} at minute {ev.get('event_minute')}")

        if appeared:
            apps += 1

    stats = {
        "player_id": player_id,
        "league_id": league_id,
        "season": season,
        "apps": apps,
        "goals": goals,
        "penalty_goals": penalty_goals,
        "assists": assists,
        "yellow_cards": yellow_cards,
        "red_cards": red_cards,
        "substitutions_in": subs_in,
        "substitutions_out": subs_out,
    }

    out_path = f"stats/{season}/{league_id}/players/{player_id}.json"
    azure_blob.upload_json(CONTAINER, out_path, stats)
    print(f"[collect_player_stats] Uploaded stats for {player_id} → {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player-id", required=True, help="Player ID")
    parser.add_argument("--league-id", required=True, help="League ID")
    parser.add_argument("--season", required=True, help="Season (e.g., 2024-2025)")
    args = parser.parse_args()

    collect_player_stats(args.player_id, args.league_id, args.season)


if __name__ == "__main__":
    main()
