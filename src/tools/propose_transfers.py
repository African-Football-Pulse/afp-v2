import os
import json
from datetime import datetime
from src.storage import azure_blob

MASTER_PATH = "players/africa/players_africa_master.json"
DRAFT_PATH = "players/africa/players_africa_master_draft.json"
TRANSFERS_DIR = "transfers"

def parse_date(date_str):
    """Convert SoccerData date (dd-mm-yyyy) to ISO string."""
    try:
        d = datetime.strptime(date_str, "%d-%m-%Y")
        return d.date().isoformat()
    except Exception:
        return None

def get_latest_season(container):
    """Hitta senaste säsongskatalogen i transfers/ baserat på namn YYYY-YYYY."""
    blobs = azure_blob.list_blobs(container, TRANSFERS_DIR)
    # plocka ut första nivån under transfers/
    seasons = set()
    for b in blobs:
        rel = b["name"].replace(TRANSFERS_DIR + "/", "")
        parts = rel.split("/")
        if len(parts) > 1 and parts[0]:
            seasons.add(parts[0])
    if not seasons:
        raise FileNotFoundError("No season directories found in transfers/")
    seasons_sorted = sorted(seasons, key=lambda s: int(s.split("-")[0]))
    return seasons_sorted[-1]

def propose_transfers():
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Läs master
    master = azure_blob.get_json(container, MASTER_PATH)
    players = master.get("players", [])
    master_by_id = {str(p["id"]): p for p in players}

    # Hitta senaste säsong
    season_name = get_latest_season(container)
    print(f"[propose_transfers] Using season {season_name}")

    # Läs alla teamfiler
    blobs = azure_blob.list_blobs(container, f"{TRANSFERS_DIR}/{season_name}")
    team_files = [b["name"] for b in blobs if b["name"].endswith(".json")]

    updates = 0
    changes = []

    for team_path in team_files:
        team_data = azure_blob.get_json(container, team_path)
        transfers_out = team_data.get("transfers", {}).get("transfers_out", [])

        # Grupp per player_id
        by_player = {}
        for t in transfers_out:
            pid = str(t.get("player_id"))
            if not pid:
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
                if d:
                    parsed.append((d, t))
            parsed.sort(key=lambda x: x[0], reverse=True)

            if not parsed:
                continue

            latest_date, latest_t = parsed[0]
            to_team = latest_t.get("to_team", {})
            to_team_name = to_team.get("name")
            to_team_id = to_team.get("id")
            transfer_type = latest_t.get("transfer_type")

            loan_status = "loan" if transfer_type == "loan" else "permanent"

            proposed = {
                "club": to_team_name,
                "club_id": to_team_id,
                "loan_status": loan_status,
                "transfer_updated": latest_date,
                "transfer_source": "SoccerData"
            }

            diff = {}
            for k, v in proposed.items():
                if player.get(k) != v:
                    diff[k] = {"old": player.get(k), "new": v}

            if diff:
                changes.append({
                    "id": pid,
                    "name": player.get("name"),
                    "diff": diff
                })

                player.update(proposed)

                if "transfer_history" not in player:
                    player["transfer_history"] = []
                for d, t in parsed:
                    hist_entry = {
                        "club": t.get("to_team", {}).get("name"),
                        "club_id": t.get("to_team", {}).get("id"),
                        "type": t.get("transfer_type"),
                        "date": d,
                        "source": "SoccerData"
                    }
                    if hist_entry not in player["transfer_history"]:
                        player["transfer_history"].append(hist_entry)

                updates += 1

    # Spara draft i Azure
    master["players"] = players
    azure_blob.put_text(container, DRAFT_PATH, json.dumps(master, indent=2, ensure_ascii=False))

    # Spara diff i Azure
    if changes:
        diff_path = f"players/africa/diff_{season_name}.json"
        azure_blob.put_text(container, diff_path, json.dumps(changes, indent=2, ensure_ascii=False))
        print(f"[propose_transfers] {updates} players updated. Draft: {DRAFT_PATH}, Diff: {diff_path}")
    else:
        print("[propose_transfers] No updates proposed.")

if __name__ == "__main__":
    propose_transfers()
