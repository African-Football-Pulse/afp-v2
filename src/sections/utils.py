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
