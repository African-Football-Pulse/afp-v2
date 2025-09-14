import os
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def merge_history():
    master = azure_blob.get_json(CONTAINER, "players/africa/players_africa_master.json")
    result = {}
    missing = {}

    # Läs in alla player_history-filer från meta/
    history_data = []
    for path in azure_blob.list_blobs(CONTAINER, "meta/"):
        if "player_history_" in path:
            data = azure_blob.get_json(CONTAINER, path)
            history_data.append(data)

    for pid, pdata in master.items():
        merged_history = []
        for season_data in history_data:
            if pid in season_data:
                merged_history.extend(season_data[pid].get("history", []))

        result[pid] = {
            "id": pid,
            "name": pdata["name"],
            "history": merged_history,
        }

        # 🔎 Debug: logga antal säsonger
        print(f"[merge_history] Player {pid} {pdata['name']} → {len(merged_history)} seasons")

        if len(merged_history) == 0:
            missing[pid] = {
                "id": pid,
                "name": pdata["name"],
            }

    # ✅ Spara sammanslagen historik
    azure_blob.upload_json(CONTAINER, "players/africa/players_africa_history.json", result)
    print("[merge_history] Uploaded merged history → players/africa/players_africa_history.json")

    # ✅ Spara separat fil med saknad historik
    if missing:
        azure_blob.upload_json(CONTAINER, "players/africa/missing_history.json", missing)
        print(f"[merge_history] Uploaded missing history → players/africa/missing_history.json "
              f"({len(missing)} players without history)")


if __name__ == "__main__":
    merge_history()
