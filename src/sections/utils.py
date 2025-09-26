# src/sections/utils.py
import os
import json
from typing import Dict, Any
from datetime import datetime
from src.producer import role_utils


def write_outputs(
    section_code: str,
    day: str,
    league: str,
    payload: Dict,
    lang: str = "en",
    status: str = "ok",
) -> Dict[str, Any]:
    """
    Writes outputs (json, md, manifest) for a given section and returns the manifest.
    Returns {"manifest": manifest} for consistency with produce_section expectations.
    """
    base_dir = f"sections/{section_code}/{day}/{league}/_"
    os.makedirs(base_dir, exist_ok=True)

    json_path = os.path.join(base_dir, "section.json")
    md_path = os.path.join(base_dir, "section.md")
    manifest_path = os.path.join(base_dir, "section_manifest.json")

    # --- Write JSON ---
    with open(json_path, "w") as f:
        json.dump(payload, f, indent=2)

    # --- Write Markdown ---
    if isinstance(payload, dict) and "script" in payload:
        md_content = payload["script"]
    else:
        md_content = f"# Section {section_code}\n\n{json.dumps(payload, indent=2)}"

    with open(md_path, "w") as f:
        f.write(md_content)

    # --- Build manifest ---
    manifest = {
        "section": section_code,
        "day": day,
        "league": league,
        "status": status,
        "lang": lang,
        "path": {
            "json": json_path,
            "md": md_path,
            "manifest": manifest_path,
        },
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[utils] Uploaded {manifest_path}")

    # ✅ Wrappa i {"manifest": manifest}
    return {"manifest": manifest}


def get_persona_block(role: str, pod: str):
    """
    Wrapper som hämtar persona_id och persona_block för en given roll och pod.
    """
    pod_cfg = role_utils.get_pod_config(pod)
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)
    persona_block = role_utils.resolve_block_for_persona(persona_id)
    return persona_id, persona_block


def load_scored_enriched(day: str, league: str = "premier_league"):
    """
    Laddar scored_enriched.jsonl från rätt path.
    """
    path = f"producer/scored/{day}/scored_enriched.jsonl"
    if not os.path.exists(path):
        raise FileNotFoundError(f"[utils] Could not find scored_enriched file at {path}")
    items = []
    with open(path, "r") as f:
        for line in f:
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return items
