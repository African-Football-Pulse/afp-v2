import os
import json
from typing import Dict
from src.storage import azure_blob
from src.producer import role_utils


def write_outputs(
    section_code: str,
    day: str,
    league: str,
    payload: Dict = None,
    status: str = "ok",
    lang: str = "en",
):
    """
    Write section outputs (json, md, manifest) to blob storage.
    Now tolerant: if day or payload is missing, we fall back gracefully.
    """
    if not section_code or not league:
        raise ValueError("[write_outputs] Missing required arguments: section_code or league")

    # Fallbacks
    if not day:
        day = "unknown"
    if not payload:
        payload = {
            "slug": "empty",
            "title": "No data",
            "text": "No data available for this section.",
            "items": [],
        }
        status = "no_data"

    # Paths
    outdir = f"sections/{section_code}/{day}/{league}/_/"

    # Write JSON
    azure_blob.upload_json("afp", f"{outdir}section.json", payload)

    # Write MD
    md_text = payload.get("text", "")
    azure_blob.put_text("afp", f"{outdir}section.md", md_text)

    # Write manifest
    manifest = {
        "section": section_code,
        "day": day,
        "league": league,
        "status": status,
        "lang": lang,
        "path": outdir,
    }
    azure_blob.upload_json("afp", f"{outdir}section_manifest.json", manifest)

    return manifest


def get_persona_block(role: str, pod: str):
    """Fetch persona_id and persona_block for given role from pod config"""
    pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
    if pod not in pods_cfg:
        raise ValueError(f"[get_persona_block] Pod '{pod}' not found in config")
    pod_cfg = pods_cfg[pod]
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)
    persona_block = role_utils.resolve_block_for_persona(persona_id)
    return persona_id, persona_block


def load_scored_enriched(day: str, league: str = "premier_league"):
    """Load scored_enriched.jsonl for a given day/league"""
    path = f"producer/scored/{day}/scored_enriched.jsonl"
    from src.producer import io_utils

    if not azure_blob.exists("afp", path):
        raise FileNotFoundError(f"[utils] Could not find scored_enriched file at {path}")

    return io_utils.read_jsonl(path)
