import os
import argparse
from src.storage import azure_blob
from src.collectors.build_player_stats import build_player

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
HISTORY_PATH = "players/africa/players_africa_history.json"


def load_all_players(container: str):
    data = azure_blob.get_json(container, HISTORY_PATH)
    return list(data.keys()), data


def run_build_player(player_id: str):
    print(f"[build_player_stats_bulk] Building stats for player {player_id}", flush=True)
    try:
        build_player(str(player_id))
        return True
    except Exception as e:
        print(f"[build_player_stats_bulk] ❌ Error building {player_id}: {e}", flush=True)
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", help="Optional limit on number of players (for testing)")
    parser.add_argument("--player-id", help="Optional single player ID to process")
    args = parser.parse_args()

    # ✅ konvertera limit om värde finns
    limit = int(args.limit) if args.limit else None

    container = CONTAINER
    players, data = load_all_players(container)

    if args.player_id:
        players = [args.player_id] if args.player_id in data else []
        print(f"[build_player_stats_bulk] Running for single player {args.player_id}", flush=True)
    else:
        print(f"[build_player_stats_bulk] Starting bulk build for {len(players)} players", flush=True)

    processed = 0
    missing = 0
    built_files = 0

    for pid in players:
        history = data.get(pid, {}).get("history", [])
        if not history:
            print(f"[build_player_stats_bulk] ⚠️ Skipping {pid}, no history", flush=True)
            missing += 1
            continue

        ok = run_build_player(pid)
        if ok:
            built_files += 1
        processed += 1

        if limit and processed >= limit:
            print(f"[build_player_stats_bulk] Limit {limit} reached, stopping.", flush=True)
            break

    print("=== Summary ===", flush=True)
    print(f"Processed players: {processed}", flush=True)
    print(f"Missing players: {missing}", flush=True)
    print(f"Built player totals: {built_files}", flush=True)
    print("[build_player_stats_bulk] DONE", flush=True)


if __name__ == "__main__":
    main()
