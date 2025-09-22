# src/sections/utils.py
import json
from pathlib import Path
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
    både lokalt och till Azure Blob.
    Returnera manifest.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    base = f"sections/{section_id}/{day}/{league}/{topic}"
    outdir = Path(base)
    outdir.mkdir(parents=True, exist_ok=True)

    # section.json
    json_path = outdir / "section.json"
    json_content = json.dumps(payload, ensure_ascii=False, indent=2)
    json_path.write_text(json_content, encoding="utf-8")
    azure_blob.upload(CONTAINER, f"{base}/section.json", json_content.encode("utf-8"))

    # section.md
    md_path = outdir / "section.md"
    title = payload.get("title", section_id)
    text = payload.get("text", "")
    md_content = f"### {title}\n\n{text}\n"
    md_path.write_text(md_content, encoding="utf-8")
    azure_blob.upload(CONTAINER, f"{base}/section.md", md_content.encode("utf-8"))

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
        },
        "metrics": {"length_s": payload.get("length_s", 0)},
        "sources": payload.get("sources", {}),
        "lang": lang,
        "status": status,
    }
    manifest_path = outdir / "section_manifest.json"
    manifest_content = json.dumps(manifest, ensure_ascii=False, indent=2)
    manifest_path.write_text(manifest_content, encoding="utf-8")
    azure_blob.upload(CONTAINER, f"{base}/section_manifest.json", manifest_content.encode("utf-8"))

    return manifest
