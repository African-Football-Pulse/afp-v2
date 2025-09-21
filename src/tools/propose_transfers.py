import os
import json
from datetime import datetime
from pathlib import Path

def parse_date(date_str):
    """Convert SoccerData date (dd-mm-yyyy) to ISO string."""
    try:
        d = datetime.strptime(date_str, "%d-%m-%Y")
        return d.date().isoformat()
    except Exception:
        return None

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_latest_season(transfers_dir="transfers"):
    """Hitta senaste säsongskatalogen i transfers/ baserat på namn YYYY-YYYY."""
    base_dir = Path(transfers_dir)
    seasons = [p for p in base_dir.iterdir() if p.is_dir()]
    if not seasons:
        raise FileNotFoundError("No season directories found in transfers/")
    # Sortera på första året i namnet
    seasons_sorted = sorted(seasons, key=lambda p: int(p.name.split("-")[0]))
    return seasons_sorted[-1]

def propose_transfers(transfers_dir="transfers", master_path="players_africa_master.json"):
    # Läs master
    master = load_json(master_path)
    players = master.get("players", [])
    master_by_id = {str(p["id"]): p for p in players}

    # Hitta senaste säsong
    season_dir = get_latest_season(transfers_dir)
    season_name = season_dir.name
    print(f"[propose_transfers] Using season {season_name}")

    team_files = list(season_dir.glob("*.json"))

    updates = 0
    changes = []

    for team_file in team_files:
        team_data = load_json(team_file)
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

            # Spara skillnader
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

                # Uppdatera i draft
                player.update(proposed)

                # Se till att historik finns
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

    # Spara draft
    draft_path = "players_africa_master_draft.json"
    master["players"] = players
    save_json(draft_path, master)

    # Spara diff-logg
    if changes:
        diff_path = f"transfers/diff_{season_name}.json"
        save_json(diff_path, changes)
        print(f"[propose_transfers] {updates} players updated. Draft: {draft_path}, Diff: {diff_path}")
    else:
        print("[propose_transfers] No updates proposed.")

if __name__ == "__main__":
    propose_transfers()
