import os
import argparse
from src.storage import azure_blob
from src.collectors.collect_player_stats import collect_player_stats

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
HISTORY_PATH = "players/africa/players_africa_history.json"


def load_history(container: str):
    """Läs in full historikfil för afrikanska spelare"""
    return azure_blob.get_json(container, HISTORY_PATH)


def run_collect_stats(player_id: str, league_id: str, season: str):
    """Anropa direkt collect_player_stats-funktionen"""
    print(f"[collect_player_stats_bulk] Collecting stats for {player_id}, league {league_id}, season {season}", flush=True)
    try:
        collect_player_stats(str(player_id), str(league_id), season)
    except Exception as e:
        print(f"[collect_player_stats_bulk] ❌ Error for {player_id}, league {league_id}, season {season}: {e}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Optional limit on number of players (for testing)")
    args = parser.parse_args()

    container = CONTAINER
    history_all = load_history(container)

    players = list(history_all.keys())
    print(f"[collect_player_stats_bulk] Starting bulk collect for {len(players)} players", flush=True)

    processed = 0
    missing = 0

    for pid, pdata in history_all.items():
        player_history = pdata.get("history", [])
        if not player_history:
            print(f"[collect_player_stats_bulk] ⚠️ Skipping {pid}, no history", flush=True)
            missing += 1
            continue

        for entry in player_history:
            season = entry["season"]
            league_id = entry["league_id"]
            run_collect_stats(pid, league_id, season)

        processed += 1

        if args.limit and processed >= args.limit:
            print(f"[collect_player_stats_bulk] Limit {args.limit} reached, stopping.", flush=True)
            break

    print("=== Summary ===", flush=True)
    print(f"Processed players: {processed}", flush=True)
    print(f"Missing players: {missing}", flush=True)
    print("[collect_player_stats_bulk] DONE", flush=True)


if __name__ == "__main__":
    main()
