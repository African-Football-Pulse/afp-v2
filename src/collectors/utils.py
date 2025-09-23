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


def get_latest_finished_date(manifest: dict) -> str:
    """
    Returnerar senaste speldatum (YYYY-MM-DD) från ett manifest.
    Filtrerar på status='finished'.
    """
    finished_matches = []

    for match in manifest.get("matches", []):
        if match.get("status") == "finished":
            raw_date = match.get("date")
            try:
                d = datetime.strptime(raw_date, "%d/%m/%Y")
                finished_matches.append(d)
            except Exception as e:
                print(f"[utils.get_latest_finished_date] ⚠️ Skippade match med fel datumformat: {raw_date} ({e})")

    if not finished_matches:
        raise ValueError("Inga färdiga matcher hittades i manifestet")

    latest = max(finished_matches)
    return latest.strftime("%Y-%m-%d")
