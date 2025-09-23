# src/collectors/utils.py
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
    Ladda ner JSON från Azure Blob + logga pathen.
    """
    try:
        data = azure_blob.get_json(CONTAINER, blob_path)
        print(f"[collectors] Downloaded {blob_path}")
        return data
    except Exception as e:
        print(f"[collectors] ⚠️ Misslyckades att ladda {blob_path}: {e}")
        return None


def get_latest_finished_date(manifest) -> str | None:
    """
    Hitta senaste matchdatum i manifest som ligger innan dagens datum.
    Stödjer SoccerData-struktur (league -> stage -> matches).
    Returnerar datumsträng 'YYYY-MM-DD' eller None.
    """
    if not manifest:
        return None

    today = datetime.utcnow().date()
    dates = []

    leagues = manifest if isinstance(manifest, list) else [manifest]

    for league in leagues:
        for stage in league.get("stage", []):
            for m in stage.get("matches", []):
                raw_date = m.get("date")
                if not raw_date:
                    continue

                # Debug: skriv ut varje datum vi hittar
                print(f"[debug] match_id={m.get('id')} raw_date={raw_date}")

                try:
                    dt = datetime.strptime(raw_date, "%d/%m/%Y").date()
                    if dt < today:
                        dates.append(dt)
                except Exception as e:
                    print(f"[debug] Kunde inte tolka datum '{raw_date}': {e}")

    if not dates:
        print("[collectors] ⚠️ Inga matchdatum före idag hittades i manifest")
        return None

    latest = max(dates)
    print(f"[collectors] ✅ Valde senaste matchdatum: {latest}")
    return latest.strftime("%Y-%m-%d")
