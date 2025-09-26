import os
import json
from typing import Dict, Any, Tuple
from src.common import blob_io
from src.producer import role_utils


def write_outputs(
    section_code: str,
    day: str,
    league: str,
    lang: str,
    pod: str,
    manifest: Dict[str, Any],
    payload: Dict[str, Any] = None,
):
    """
    Skriver outputs (JSON, MD, manifest) till blob och returnerar manifest.
    """
    if not section_code or not day or not league or not lang:
        raise ValueError("[write_outputs] Missing required arguments")

    base_path = f"sections/{section_code}/{day}/{league}/_/"

    # JSON
    json_path = base_path + "section.json"
    blob_io.upload_json(json_path, manifest)

    # MD
    md_path = base_path + "section.md"
    blob_io.upload_text(md_path, manifest.get("script", ""))

    # Manifest
    manifest_path = base_path + "section_manifest.json"
    manifest_obj = {
        "section": section_code,
        "day": day,
        "league": league,
        "status": "done",
        "lang": lang,
        "path": base_path,
    }
    blob_io.upload_json(manifest_path, manifest_obj)

    print(f"[utils] Uploaded {manifest_path}")
    return {
        "section": section_code,
        "day": day,
        "league": league,
        "status": "done",
        "lang": lang,
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
        text = blob_io.download_text(path)
    except Exception as e:
        raise FileNotFoundError(f"[utils] Could not load scored_enriched from blob: {path}") from e

    items = []
    for line in text.splitlines():
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return items
