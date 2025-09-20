import os
from src.storage import azure_blob


MASTER_PATH = "players/africa/players_africa_master.json"
OUTPUT_PATH = "players/africa/players_africa_history.json"
MISSING_PATH = "players/africa/missing_history.json"


def list_meta_paths(container: str):
    """Hitta alla player_history-filer i meta/ rekursivt."""
    paths = []
    blob_list = azure_blob.list_prefix(container, "meta/")
    for blob_name in blob_list:
        if "player_history_" in blob_name and blob_name.endswith(".json"):
            paths.append(blob_name)
    return paths


def load_master(container: str):
    data = azure_blob.get_json(container, MASTER_PATH)
    if isinstance(data, dict) and "players" in data:
        return data["players"]
    return data


def merge_history(container: str):
    master = load_master(container)

    africa_ids = {
        str(p["id"]): p["name"]
        for p in master
        if str(p.get("id", "")).isdigit()
    }

    history_total = {
        pid: {"id": pid, "name": pname, "history": []}
        for pid, pname in africa_ids.items()
    }

    for path in list_meta_paths(container):
        data = azure_blob.get_json(container, path)
        if not data:
            continue

        for pid, pdata in data.items():
            if pid in history_total:
                for entry in pdata.get("history", []):
                    if entry not in history_total[pid]["history"]:
                        history_total[pid]["history"].append(entry)

    missing = {}
    for pid, pdata in history_total.items():
        seasons = len(pdata["history"])
        print(f"[merge_africa_player_history] Player {pid} {pdata['name']} → {seasons} seasons", flush=True)
        if seasons == 0:
            missing[pid] = {"id": pid, "name": pdata["name"]}

    azure_blob.upload_json(container, OUTPUT_PATH, history_total)
    print(f"[merge_africa_player_history] Uploaded → {OUTPUT_PATH}", flush=True)
    print(f"[merge_africa_player_history] Players included: {len(history_total)}", flush=True)

    if missing:
        azure_blob.upload_json(container, MISSING_PATH, missing)
        print(f"[merge_africa_player_history] Uploaded missing history → {MISSING_PATH} "
              f"({len(missing)} players without history)", flush=True)

    print(f"[merge_africa_player_history] DONE. Players total={len(history_total)}, missing={len(missing)}", flush=True)


def main():
    container = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
    merge_history(container)


if __name__ == "__main__":
    main()
