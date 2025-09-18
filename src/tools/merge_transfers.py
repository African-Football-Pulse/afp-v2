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
        club_name = club["name"]

        for t in transfers.get("transfers", {}).get("transfers_out", []):
            pid = t.get("player_id")
            if not pid:
                continue
            if player_id and pid != player_id:
                continue

            if pid not in master_by_id:
                continue  # vi bryr oss bara om spelare i master

            player = master_by_id[pid]
            transfer_date = parse_date(t.get("transfer_date"))
            transfer_type = t.get("transfer_type")
            to_team = t.get("to_team", {}).get("name")

            # Debug – skriv alltid ut full transfer-post om player_id är satt
            if player_id:
                print("[merge_transfers] Transfer record from SoccerData:")
                print(json.dumps(t, indent=2, ensure_ascii=False))

            print(f"[merge_transfers] Checking player {pid} ({player['name']})")
            print(f"- Current master: club={player.get('club')}, loan_status={player.get('loan_status')}, transfer_updated={player.get('transfer_updated')}")

            # Datumkontroll
            current_updated = player.get("transfer_updated")
            if current_updated and transfer_date and transfer_date <= current_updated:
                print(f"⚠️ Skipping: transfer {transfer_date} is older or same as current {current_updated}")
                continue

            # Bestäm loan_status + parent_club
            if transfer_type == "loan":
                loan_status = "loan"
                parent_club = club_name
            else:
                loan_status = "permanent"
                parent_club = None

            # Uppdatera staging-data
            player["loan_status"] = loan_status
            player["parent_club"] = parent_club
            player["club"] = to_team
            player["transfer_source"] = "SoccerData"
            player["transfer_updated"] = transfer_date

            # Lägg till historik
            hist_entry = {
                "club": to_team,
                "type": transfer_type,
                "date": transfer_date,
                "source": "SoccerData"
            }
            if "transfer_history" not in player:
                player["transfer_history"] = []
            player["transfer_history"].append(hist_entry)

            print(f"✅ Updating draft: loan_status={loan_status}, club={to_team}, parent_club={parent_club}, transfer_updated={transfer_date}")
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
