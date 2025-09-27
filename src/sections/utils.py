import os
import json
from typing import Dict, Any, Tuple
from src.storage import azure_blob
from src.producer import role_utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def write_outputs(
    section_code: str,
    day: str,
    league: str,
    lang: str,
    pod: str,
    manifest: Dict[str, Any],
    status: str = "done",
    payload: Dict[str, Any] = None,
):
    """
    Skriver outputs (JSON, MD, manifest) till Azure Blob och returnerar manifest.
    """
    if not section_code or not day or not league or not lang:
        raise ValueError("[write_outputs] Missing required arguments")

    base_path = f"sections/{section_code}/{day}/{league}/{pod}"

    # JSON (med manifest + payload)
    json_path = f"{base_path}/section.json"
    data = {
        "section": section_code,
        "day": day,
        "league": league,
        "status": status,
        "lang": lang,
        "pod": pod,
        "manifest": manifest,
        "payload": payload or {},
    }
    azure_blob.upload_json(CONTAINER, json_path, data)

    # MD (ren text)
    md_path = f"{base_path}/section.md"
    azure_blob.put_text(CONTAINER, md_path, manifest.get("script", ""))

    # Manifest (metadata)
    manifest_path = f"{base_path}/section_manifest.json"
    manifest_obj = {
        "section": section_code,
        "day": day,
        "league": league,
        "status": status,
        "lang": lang,
        "pod": pod,
        "path": base_path,
    }
    azure_blob.upload_json(CONTAINER, manifest_path, manifest_obj)

    print(
        f"[utils] 📤 Wrote outputs for {section_code} → {base_path} "
        f"(json={json_path}, md={md_path}, manifest={manifest_path}, status={status})"
    )

    return {
        "section": section_code,
        "day": day,
        "league": league,
        "status": status,
        "lang": lang,
        "pod": pod,
        "path": base_path,
        "manifest": manifest,
        "payload": payload,
    }


def get_persona_block(role: str, pod: str) -> Tuple[str, Dict[str, Any]]:
    """
    Hämtar persona-id och block för en given roll i en pod.
    """
    pod_cfg = role_utils.get_pod_config(pod)
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)
    persona_block = role_utils.resolve_block_for_persona(persona_id)
    return persona_id, persona_block


def load_scored_enriched(day: str, league: str = "premier_league"):
    """
    Laddar scored_enriched.jsonl från Azure blob.
    """
    path = f"producer/scored/{day}/scored_enriched.jsonl"
    try:
        text = azure_blob.get_text(CONTAINER, path)
    except Exception as e:
        raise FileNotFoundError(f"[utils] Could not load scored_enriched from blob: {path}") from e

    items = []
    for line in text.splitlines():
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return items
