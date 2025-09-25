import os
import json
from typing import List, Dict
from src.storage import azure_blob
from src.producer import role_utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def load_news_items(feed: str, league: str, day: str) -> List[Dict]:
    """
    Load curated news items from Azure for a given feed/league/day.
    """
    path = f"curated/news/{feed}/{league}/{day}/items.json"
    if not azure_blob.exists(CONTAINER, path):
        return []
    return azure_blob.get_json(CONTAINER, path)


def load_candidates(day: str, news_arg: str = None) -> List[Dict]:
    """
    Load candidate/scored items for a given day.
    Priority:
    1. Explicit path via news_arg
    2. scored_enriched.jsonl
    3. scored.jsonl
    Returns a list of dicts.
    """
    import jsonlines

    if news_arg:
        blob_path = news_arg
    else:
        blob_path = f"producer/scored/{day}/scored_enriched.jsonl"
        if not azure_blob.exists(CONTAINER, blob_path):
            blob_path = f"producer/scored/{day}/scored.jsonl"
            if not azure_blob.exists(CONTAINER, blob_path):
                return []

    if not azure_blob.exists(CONTAINER, blob_path):
        return []

    text = azure_blob.get_text(CONTAINER, blob_path)
    items = []
    with jsonlines.Reader(text.splitlines()) as reader:
        for obj in reader:
            items.append(obj)
    return items


def write_outputs(section_code: str, league: str, lang: str, day: str, payload: Dict, path_scope: str = "blob"):
    """
    Standardized output writer for sections.
    Writes JSON, MD, and manifest to Azure.
    """
    base_path = f"sections/{section_code}/{day}/{league}/_/"

    # JSON
    azure_blob.upload_json(CONTAINER, base_path + "section.json", payload)

    # MD
    if "text" in payload:
        azure_blob.put_text(CONTAINER, base_path + "section.md", payload["text"])

    # Manifest
    manifest = {
        "section_code": section_code,
        "league": league,
        "lang": lang,
        "day": day,
        "title": payload.get("title", ""),
        "type": payload.get("type", ""),
        "sources": payload.get("sources", {}),
    }
    azure_blob.upload_json(CONTAINER, base_path + "section_manifest.json", manifest)

    print(f"[utils] Uploaded {base_path}section.json")
    print(f"[utils] Uploaded {base_path}section.md")
    print(f"[utils] Uploaded {base_path}section_manifest.json")

    return manifest


def get_persona_block(role: str, pod: str):
    """
    Returnera (persona_id, persona_block) för en given roll och pod.
    - Läser pods.yaml för role_map.
    - Slår upp persona i personas.json.
    - Bygger ett block som kan användas i GPT-prompt.
    - Har fallback till 'news_anchor' om inget hittas.
    """
    # Ladda pod config
    pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
    pod_cfg = pods_cfg.get(pod, {})

    # Resolve persona_id via role_map
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)
    if not persona_id:
        persona_id = "news_anchor"

    # Ladda personas
    personas = role_utils.load_yaml("config/personas.json")
    persona_cfg = personas.get(persona_id, {})

    # Bygg block
    persona_block = f"""{persona_cfg.get("name", persona_id)}
Role: {persona_cfg.get("role", "News Anchor")}
Voice: {persona_cfg.get("voice", "neutral")}
Tone: {persona_cfg.get("tone", {}).get("primary", "informative")}
Style: {persona_cfg.get("style", "clear")}
"""

    return persona_id, persona_block
