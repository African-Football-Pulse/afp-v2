import os
import json
import argparse
from datetime import datetime
from src.storage import azure_blob

def parse_date(date_str):
    """Convert SoccerData date (dd-mm-yyyy) to ISO string."""
    try:
        d = datetime.strptime(date_str, "%d-%m-%Y")
        return d.date().isoformat()
    except Exception:
        return None

def merge_transfers(league_id: int, season: str, player_id: int = None):
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Läs masterfil
    master_path = "players/africa/players_africa_master.json"
    master = azure_blob.get_json(container, master_path)
    players = master.get("players", [])
    master_by_id = {p["id"]: p for p in players}

    # Läs manifest
    manifest_path = f"transfers/{league_id}/manifest.json"
    manifest = azure_blob.get_json(container, manifest_path)

    updates = 0

    for club in manifest["teams"]:
        transfers = azure_blob.get_json(container, club["path"])
        transfers_out = transfers.get("transfers", {}).get("transfers_out", [])

        # Grupp per player_id
        by_player = {}
        for t in transfers_out:
            pid = t.get("player_id")
            if not pid:
                continue
            if player_id and pid != player_id:
                continue
            by_player.setdefault(pid, []).append(t)

        for pid, t_list in by_player.items():
            if pid not in master_by_id:
                continue

            player = master_by_id[pid]

            # Sortera transfers efter datum
            parsed = []
            for t in t_list:
                d = parse_date(t.get("transfer_date"))
                parsed.append((d, t))
            parsed = [x for x in parsed if x[0] is not None]
            parsed.sort(key=lambda x: x[0], reverse=True)

            if not parsed:
                continue

            latest_date, latest_t = parsed[0]
            to_team = latest_t.get("to_team", {}).get("name")
            transfer_type = latest_t.get("transfer_type")

            # Debug: dumpa hela transferlistan om player_id är satt
            if player_id:
                print(f"[merge_transfers] Transfers for {player['name']} ({pid}):")
                for d, t in parsed:
                    print(json.dumps(t, indent=2, ensure_ascii=False))

            print(f"[merge_transfers] Checking player {pid} ({player['name']})")
            print(f"- Current master: club={player.get('club')}, loan_status={player.get('loan_status')}, transfer_updated={player.get('transfer_updated')}")

            # Datumkontroll mot master
            current_updated = player.get("transfer_updated")
            if current_updated and latest_date and latest_date <= current_updated:
                print(f"⚠️ Skipping: latest transfer {latest_date} is older or same as current {current_updated}")
                continue

            # Bestäm loan_status + parent_club
            if transfer_type == "loan":
                loan_status = "loan"
                parent_club = club["name"]
            else:
                loan_status = "permanent"
                parent_club = None

            # Uppdatera master med senaste transfern
            player["loan_status"] = loan_status
            player["parent_club"] = parent_club
            player["club"] = to_team
            player["transfer_source"] = "SoccerData"
            player["transfer_updated"] = latest_date

            # Se till att transfer_history finns
            if "transfer_history" not in player:
                player["transfer_history"] = []

            # Lägg in alla transfers i historiken
            for d, t in parsed:
                hist_entry = {
                    "club": t.get("to_team", {}).get("name"),
                    "type": t.get("transfer_type"),
                    "date": d,
                    "source": "SoccerData"
                }
                if hist_entry not in player["transfer_history"]:
                    player["transfer_history"].append(hist_entry)

            print(f"✅ Updating draft: loan_status={loan_status}, club={to_team}, parent_club={parent_club}, transfer_updated={latest_date}")
            updates += 1

    # Spara till draft-fil (inte master direkt!)
    if updates > 0:
        draft_path = "players/africa/players_africa_master_draft.json"
        master["players"] = players
        azure_blob.put_text(container, draft_path, json.dumps(master, indent=2, ensure_ascii=False))
        print(f"[merge_transfers] Saved {updates} updates → {draft_path}")
        print("⚠️ NOTE: Master file not modified. Review draft before promoting to master.")
    else:
        print("[merge_transfers] No updates applied.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league-id", type=int, default=228)
    parser.add_argument("--season", type=str, default="2024-2025")
    parser.add_argument("--player-id", type=int, help="Optional: limit to one player")
    args = parser.parse_args()

    merge_transfers(args.league_id, args.season, args.player_id)
