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
    parser.add_argument("--player-id", help="Optional single player ID to process")
    args = parser.parse_args()

    # ✅ limit som int om värde finns
    limit = int(args.limit) if args.limit else None

    container = CONTAINER
    history_all = load_history(container)

    if args.player_id:
        # Bearbeta bara en specifik spelare
        players = [args.player_id] if args.player_id in history_all else []
        print(f"[collect_player_stats_bulk] Running for single player {args.player_id}", flush=True)
    else:
        # Bearbeta alla spelare
        players = list(history_all.keys())
        print(f"[collect_player_stats_bulk] Starting bulk collect for {len(players)} players", flush=True)

    processed = 0
    missing = 0
    created_files = 0

    for pid in players:
        pdata = history_all.get(pid, {})
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
