import os
import argparse
from src.storage import azure_blob
from src.collectors.collect_player_stats import collect_player_stats

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
HISTORY_PATH = "players/africa/players_africa_history.json"


def load_history(container: str):
    return azure_blob.get_json(container, HISTORY_PATH)


def run_collect_stats(player_id: str, league_id: str, season: str):
    print(f"[collect_player_stats_bulk] Collecting stats for {player_id}, league {league_id}, season {season}", flush=True)
    try:
        collect_player_stats(str(player_id), str(league_id), season)
        return True
    except Exception as e:
        print(f"[collect_player_stats_bulk] ❌ Error for {player_id}, league {league_id}, season {season}: {e}", flush=True)
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", help="Optional limit on number of players (for testing)")
    args = parser.parse_args()

    # ✅ omvandla till int om värde finns, annars None
    limit = int(args.limit) if args.limit else None

    container = CONTAINER
    history_all = load_history(container)
    players = list(history_all.keys())

    print(f"[collect_player_stats_bulk] Starting bulk collect for {len(players)} players", flush=True)

    processed = 0
    missing = 0
    created_files = 0

    for pid, pdata in history_all.items():
        player_history = pdata.get("history", [])
        if not player_history:
            print(f"[collect_player_stats_bulk] ⚠️ Skipping {pid}, no history", flush=True)
            missing += 1
            continue

        for entry in player_history:
            season = entry["season"]
            league_id = entry["league_id"]
            ok = run_collect_stats(pid, league_id, season)
            if ok:
                created_files += 1

        processed += 1

        if limit and processed >= limit:
            print(f"[collect_player_stats_bulk] Limit {limit} reached, stopping.", flush=True)
            break

    print("=== Summary ===", flush=True)
    print(f"Processed players: {processed}", flush=True)
    print(f"Missing players: {missing}", flush=True)
    print(f"Created stats files: {created_files}", flush=True)
    print("[collect_player_stats_bulk] DONE", flush=True)


if __name__ == "__main__":
    main()
