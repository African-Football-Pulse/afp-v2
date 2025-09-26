import os
from typing import Dict
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def write_outputs(
    section_code: str,
    league: str,
    lang: str,
    day: str,
    payload: Dict,
    path_scope: str = "blob",
    status: str = "ok"
):
    """
    Standardized output writer for sections.
    Writes JSON, MD, and manifest to Azure Blob Storage.
    """

    base_path = f"sections/{section_code}/{day}/{league}/_/"

    # Ensure status is also reflected in payload.meta
    if "meta" not in payload:
        payload["meta"] = {}
    payload["meta"]["status"] = status

    # Write section.json
    azure_blob.upload_json(CONTAINER, base_path + "section.json", payload)

    # Write section.md (if text exists)
    if "text" in payload:
        azure_blob.put_text(CONTAINER, base_path + "section.md", payload["text"])

    # Build and write manifest
    manifest = {
        "section_code": section_code,
        "league": league,
        "lang": lang,
        "day": day,
        "title": payload.get("title", ""),
        "type": payload.get("type", ""),
        "sources": payload.get("sources", {}),
        "status": status,
    }
    azure_blob.upload_json(CONTAINER, base_path + "section_manifest.json", manifest)

    print(f"[utils] Uploaded {base_path}section.json")
    if "text" in payload:
        print(f"[utils] Uploaded {base_path}section.md")
    print(f"[utils] Uploaded {base_path}section_manifest.json")

    return manifest
