import os
import json
import datetime
from typing import Dict, Any, List, Tuple
from src.common import blob_io
from src.producer import role_utils


def write_outputs(section_code: str, day: str, league: str, lang: str, pod: str, manifest: Dict[str, Any]):
    """
    Skriver ut outputs (JSON, MD, manifest) till blob.
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
    blob_io.upload_json(manifest_path, {
        "section": section_code,
        "day": day,
        "league": league,
        "status": "done",
        "lang": lang,
        "path": base_path
    })

    print(f"[utils] Uploaded {manifest_path}")
    return {
        "section": section_code,
        "day": day,
        "league": league,
        "status": "done",
        "lang": lang,
        "path": base_path,
    }


def get_today() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")


def get_persona_block(role: str, pod: str) -> Tuple[str, Dict[str, Any]]:
    """
    Hämtar persona-id och block baserat på roll och pod.
    """
    pod_cfg = role_utils.get_pod_config(pod)
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)
    persona_block = role_utils.resolve_block_for_persona(persona_id)
    return persona_id, persona_block


def load_scored_enriched(day: str, league: str = "premier_league") -> List[Dict]:
    """
    Laddar scored_enriched.jsonl från Azure Blob Storage.
    """
    blob_path = f"producer/scored/{day}/scored_enriched.jsonl"

    print(f"[utils] 🔗 Laddar scored_enriched från blob: {blob_path}")
    data = blob_io.load_blob_as_text(blob_path)

    items = []
    for line in data.splitlines():
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    print(f"[utils] ✅ Laddade {len(items)} items från blob {blob_path}")
    return items
