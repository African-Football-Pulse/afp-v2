import os
import json
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

MASTER_PATH = "players/africa/players_africa_master.json"
OUTPUT_PATH = "players/africa/players_africa_history.json"


def list_meta_paths():
    """Hitta alla player_history-filer i meta/."""
    paths = []
    blob_list = azure_blob.list_blobs(CONTAINER, "meta/")
    for blob in blob_list:
        if "player_history_" in blob.name and blob.name.endswith(".json"):
            paths.append(blob.name)
    return paths


def load_master():
    """Läs masterfilen med våra African players."""
    data = azure_blob.get_json(CONTAINER, MASTER_PATH)
    if isinstance(data, dict) and "players" in data:
        return data["players"]
    return data


def merge_history():
    master = load_master()

    # Endast spelare med numeriska ID (skip AFR00X)
    africa_ids = {
        str(p["id"]): p["name"]
        for p in master
        if str(p.get("id", "")).isdigit()
    }

    history_total = {
        pid: {"id": pid, "name": pname, "history": []}
        for pid, pname in africa_ids.items()
    }

    for path in list_meta_paths():
        data = azure_blob.get_json(CONTAINER, path)
        if not data:
            continue

        for pid, pdata in data.items():
            if pid in history_total:
                for entry in pdata.get("history", []):
                    if entry not in history_total[pid]["history"]:
                        history_total[pid]["history"].append(entry)

    azure_blob.upload_json(CONTAINER, OUTPUT_PATH, history_total)
    print(f"[merge_africa_player_history] Uploaded → {OUTPUT_PATH}")
    print(f"[merge_africa_player_history] Players included: {len(history_total)}")


def main():
    merge_history()


if __name__ == "__main__":
    main()
