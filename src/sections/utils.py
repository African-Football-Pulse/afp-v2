# src/sections/utils.py

import os
import json
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")


def write_outputs(container: str,
                  prefix: str,
                  section_id: str,
                  text: str,
                  manifest: dict):
    """
    Skriv ut standardiserade filer för en sektion:
      - section.md (textinnehåll)
      - section.json (JSON med text)
      - section_manifest.json (metadata/manifest)
    Alla filer läggs i Azure under angivet prefix.
    """
    # Paths
    base = f"{prefix}/{section_id}"
    md_path = f"{base}/section.md"
    json_path = f"{base}/section.json"
    manifest_path = f"{base}/section_manifest.json"

    # Skriv text som markdown
    azure_blob.put_text(container, md_path, text, content_type="text/markdown; charset=utf-8")

    # Skriv text som JSON
    obj = {"text": text}
    azure_blob.upload_json(container, json_path, obj)

    # Skriv manifest
    azure_blob.upload_json(container, manifest_path, manifest)

    return {
        "md": md_path,
        "json": json_path,
        "manifest": manifest_path,
    }


def load_news_items(container: str, blob_path: str):
    """
    Hjälpfunktion för NEWS-sektioner som laddar scorade kandidater.
    """
    if not azure_blob.exists(container, blob_path):
        return []
    data = azure_blob.get_json(container, blob_path)
    return data
