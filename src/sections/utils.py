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


# --- Persona + Scored helpers ---
from src.producer import role_utils


def get_persona_block(role: str, pod: str):
    """
    Wrapper for role_utils to fetch persona_id and persona_block
    for a given role in a given pod.
    """
    pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
    pod_cfg = pods_cfg.get(pod, {})
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)
    persona_block = role_utils.resolve_block_for_persona(persona_id)
    return persona_id, persona_block


def load_scored_enriched(day: str):
    """
    Wrapper for role_utils.load_scored_enriched so sections can just call utils.load_scored_enriched.
    Returns a list of scored_enriched items for the given day.
    """
    return role_utils.load_scored_enriched(day)
