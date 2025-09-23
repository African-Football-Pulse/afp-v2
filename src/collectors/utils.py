import os
from datetime import datetime
from src.storage import azure_blob

# Container sätts från env, annars fallback
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def upload_json_debug(blob_path: str, obj):
    """
    Ladda upp JSON till Azure Blob + logga pathen.
    """
    azure_blob.upload_json(CONTAINER, blob_path, obj)
    print(f"[collectors] Uploaded {blob_path}")


def upload_text_debug(blob_path: str, text: str, content_type: str = "text/plain; charset=utf-8"):
    """
    Ladda upp text till Azure Blob + logga pathen.
    """
    azure_blob.put_bytes(CONTAINER, blob_path, text.encode("utf-8"), content_type=content_type)
    print(f"[collectors] Uploaded {blob_path}")


def download_json_debug(blob_path: str):
    """
    Läs JSON från Azure Blob + logga pathen.
    Returnerar None om filen inte finns.
    """
    try:
        obj = azure_blob.download_json(CONTAINER, blob_path)
        print(f"[collectors] Downloaded {blob_path}")
        return obj
    except Exception as e:
        print(f"[collectors] ⚠️ Misslyckades att ladda {blob_path}: {e}")
        return None


def get_latest_finished_date(manifest: dict) -> str | None:
    """
    Hitta senaste färdiga matchdatum från ett manifest.
    Returnerar YYYY-MM-DD eller None om inget hittas.
    """
    latest = None
    for m in manifest.get("matches", []):
        if m.get("status") == "finished":
            try:
                dt = datetime.strptime(m["date"], "%d/%m/%Y")
                if latest is None or dt > latest:
                    latest = dt
            except Exception:
                continue
    return latest.strftime("%Y-%m-%d") if latest else None
