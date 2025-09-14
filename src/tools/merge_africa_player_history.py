import os
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

MASTER_PATH = "players/africa/players_africa_master.json"
OUTPUT_PATH = "players/africa/players_africa_history.json"
MISSING_PATH = "players/africa/missing_history.json"


def list_meta_paths():
    """Hitta alla player_history-filer i meta/."""
    paths = []
    blob_list = azure_blob.list_prefix(CONTAINER, "meta/")
    for blob_name in blob_list:
        if "player_history_" in blob_name and blob_name.endswith(".json"):
            paths.append(blob_name)
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

    # Debug: logga antal säsonger per spelare
    missing = {}
    for pid, pdata in history_total.items():
        seasons = len(pdata["history"])
        print(f"[merge_africa_player_history] Player {pid} {pdata['name']} → {seasons} seasons")
        if seasons == 0:
            missing[pid] = {"id": pid, "name": pdata["name"]}

    # Spara historikfil
    azure_blob.upload_json(CONTAINER, OUTPUT_PATH, history_total)
    print(f"[merge_africa_player_history] Uploaded → {OUTPUT_PATH}")
    print(f"[merge_africa_player_history] Players included: {len(history_total)}")

    # Spara missing_history
    if missing:
        azure_blob.upload_json(CONTAINER, MISSING_PATH, missing)
        print(f"[merge_africa_player_history] Uploaded missing history → {MISSING_PATH} "
              f"({len(missing)} players without history)")


def main():
    merge_history()


if __name__ == "__main__":
    main()
