import os
import json
from collections import defaultdict
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def generate_club_index():
    master_path = "players/africa/players_africa_master.json"
    master = azure_blob.get_json(CONTAINER, master_path)

    if not master or "players" not in master:
        raise RuntimeError(f"[generate_club_index] Missing or invalid master file at {master_path}")

    club_index = defaultdict(list)

    for p in master["players"]:
        club = p.get("club", "").strip()
        if not club:
            continue
        club_index[club].append({
            "id": p.get("id"),
            "name": p["name"],
            "short_aliases": p.get("short_aliases", []),
            "aliases": p.get("aliases", []),
            "country": p.get("country")
        })

    index_path = "players/africa/players_club_index.json"
    azure_blob.upload_json(CONTAINER, index_path, club_index)

    # Debug / summary output
    print(f"[generate_club_index] Uploaded → {index_path}")
    print(f"[generate_club_index] Indexed {len(club_index)} clubs")
    for club, players in list(club_index.items())[:10]:  # visa max 10 klubbar
        print(f"  {club}: {len(players)} spelare")
    if len(club_index) > 10:
        print("  ...")


def main():
    print("🔄 Generating club index...")
    generate_club_index()
    print("✅ Club index generation complete.")


if __name__ == "__main__":
    main()
