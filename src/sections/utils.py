# src/sections/utils.py
import json
from datetime import datetime, timezone
from typing import Dict, Any
import os

from src.storage import azure_blob

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")


def write_outputs(
    section_id: str,
    day: str,
    league: str,
    payload: Dict[str, Any],
    status: str,
    topic: str = "_",
    lang: str = "en",
) -> Dict[str, Any]:
    """
    Skriv ut section.json, section.md, section_manifest.json
    direkt till Azure Blob Storage med azure_blob helpers.
    Returnera manifest.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    base = f"sections/{section_id}/{day}/{league}/{topic}"

    # section.json
    azure_blob.upload_json(CONTAINER, f"{base}/section.json", payload)
    print(f"[utils] Uploaded {base}/section.json")

    # section.md
    title = payload.get("title", section_id)
    text = payload.get("text", "")
    md_content = f"### {title}\n\n{text}\n"
    azure_blob.put_text(CONTAINER, f"{base}/section.md", md_content)
    print(f"[utils] Uploaded {base}/section.md")

    # manifest
    manifest = {
        "section_code": section_id,
        "type": payload.get("type", "news"),
        "model": payload.get("model", "gpt-4o-mini"),
        "created_utc": ts,
        "league": league,
        "topic": topic,
        "date": day,
        "blobs": {
            "json": f"{base}/section.json",
            "md": f"{base}/section.md",
            "manifest": f"{base}/section_manifest.json",
        },
        "metrics": {"length_s": payload.get("length_s", 0)},
        "sources": payload.get("sources", {}),
        "lang": lang,
        "status": status,
    }
    azure_blob.upload_json(CONTAINER, f"{base}/section_manifest.json", manifest)
    print(f"[utils] Uploaded {base}/section_manifest.json")

    return manifest

def load_candidates(day: str, news_arg: str = None):
    """
    Hämta scored candidates från Azure Blob.
    Returnerar (candidates, blob_path).
    """
    blob_path = news_arg if news_arg else f"producer/candidates/{day}/scored.jsonl"
    try:
        text = azure_blob.get_text(CONTAINER, blob_path)
        candidates = [json.loads(line) for line in text.splitlines() if line.strip()]
        return candidates, blob_path
    except Exception as e:
        print(f"[utils] WARN: could not load candidates from {blob_path} ({e})")
        return [], blob_path


def load_news_items(feed: str, league: str, day: str):
    """
    Ladda nyhets-items från curated/news/<feed>/<league>/<day>/items.json
    Returnerar en lista med items eller [] om inget hittas.
    """
    path = f"curated/news/{feed}/{league}/{day}/items.json"
    try:
        return azure_blob.get_json(CONTAINER, path)
    except Exception as e:
        print(f"[utils] WARN: could not load news items from {path} ({e})")
        return []
