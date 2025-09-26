import os
import json
from typing import Dict
from src.storage import azure_blob
from src.producer import role_utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def write_outputs(
    section_code: str,
    *args,
    league: str = None,
    lang: str = "en",
    day: str = None,
    payload: Dict = None,
    path_scope: str = "blob",
    status: str = "ok"
):
    """
    Standardized output writer for sections.
    Supports both legacy positional calls and keyword calls.
    """

    if args:
        if len(args) == 3:  # (day, league, payload)
            day, league, payload = args
        elif len(args) == 4:  # (league, lang, day, payload)
            league, lang, day, payload = args
        else:
            raise ValueError(f"[write_outputs] Unexpected positional args: {args}")

    if not all([section_code, day, league, payload]):
        raise ValueError("[write_outputs] Missing required arguments")

    base_path = f"sections/{section_code}/{day}/{league}/_/"

    if "meta" not in payload:
        payload["meta"] = {}
    payload["meta"]["status"] = status

    azure_blob.upload_json(CONTAINER, base_path + "section.json", payload)

    if "text" in payload:
        azure_blob.put_text(CONTAINER, base_path + "section.md", payload["text"])

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
    pods_cfg = role_utils.load_yaml("config/pods.yaml")["pods"]
    pod_cfg = pods_cfg.get(pod, {})
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)

    roles_cfg = role_utils.load_yaml("config/speaking_roles.yaml")["roles"]

    persona_block = None
    for role_name, langs in roles_cfg.items():
        for lang_key, val in langs.items():
            if isinstance(val, dict):
                if persona_id in val.values():
                    persona_block = f"{role_name}:{persona_id}"
            else:
                if persona_id == val:
                    persona_block = f"{role_name}:{persona_id}"

    if persona_block is None:
        persona_block = persona_id

    return persona_id, persona_block


def load_scored_enriched(day: str, league: str = "premier_league", container: str = CONTAINER):
    """
    Load scored_enriched.jsonl for given day from Azure Blob.
    Returns a list of dicts.
    """
    blob_path = f"producer/scored/{day}/scored_enriched.jsonl"
    try:
        text = azure_blob.get_text(container, blob_path)
    except Exception as e:
        raise FileNotFoundError(f"[utils] Could not fetch {blob_path} from Azure Blob: {e}")

    items = []
    for line in text.splitlines():
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    print(f"[utils] Loaded {len(items)} items from {blob_path}")
    return items
