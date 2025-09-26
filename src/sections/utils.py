import os
from typing import Dict
import yaml
from src.storage import azure_blob
from src.producer import role_utils

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


def get_persona_block(role: str, pod: str):
    """
    Resolve persona_id via pods.yaml and fetch corresponding block
    from speaking_roles.yaml.
    Returns (persona_id, persona_block).
    """
    # Hämta persona_id från pods.yaml
    pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
    pod_cfg = pods_cfg.get(pod, {})
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)

    # Hämta block från speaking_roles.yaml
    roles_cfg = role_utils.load_yaml("config/speaking_roles.yaml")["roles"]

    persona_block = None
    for role_name, langs in roles_cfg.items():
        for lang_key, val in langs.items():
            if isinstance(val, dict):
                # ex: duo_experts -> { expert1: ak, expert2: jjk }
                if persona_id in val.values():
                    persona_block = f"{role_name}:{persona_id}"
            else:
                # ex: single_expert -> en: ak
                if persona_id == val:
                    persona_block = f"{role_name}:{persona_id}"

    if persona_block is None:
        persona_block = persona_id  # fallback

    return persona_id, persona_block


def load_scored_enriched(day: str):
    """
    Wrapper for role_utils.load_scored_enriched so sections can just call utils.load_scored_enriched.
    Returns a list of scored_enriched items for the given day.
    """
    return role_utils.load_scored_enriched(day)
