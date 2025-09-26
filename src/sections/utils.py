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
    payload: Dict[str, Any],
    *,
    status: str = "success",
    lang: str = "en",
    path_scope: str = "local",
    outdir: str = "sections",
    pod: str = None,
) -> Dict[str, Any]:
    """
    Skriver ut JSON/MD/manifest för en sektion och returnerar manifestet.
    Gör nu argumenten mer toleranta: kräver bara section_code, day, league, payload.
    """

    # fallback om något saknas
    section_code = section_code or "UNKNOWN.SECTION"
    day = day or datetime.today().strftime("%Y-%m-%d")
    league = league or "unknown_league"
    lang = lang or "en"

    section_path = f"{outdir}/{section_code}/{day}/{league}/_"

    os.makedirs(section_path, exist_ok=True)

    # JSON
    json_path = os.path.join(section_path, "section.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # MD (för snabb läsning)
    md_path = os.path.join(section_path, "section.md")
    with open(md_path, "w", encoding="utf-8") as f:
        if "script" in payload:
            f.write(payload["script"])
        else:
            f.write(f"# {section_code}\n\n(no script generated)\n")

    # Manifest
    manifest = {
        "section": section_code,
        "day": day,
        "league": league,
        "status": status,
        "lang": lang,
        "path": section_path,
    }

    manifest_path = os.path.join(section_path, "section_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"[utils] Uploaded {manifest_path}")
    return manifest


def load_scored_enriched(day: str, base_path: str = "producer/scored") -> list[dict]:
    """
    Laddar scored_enriched.jsonl för ett visst datum.
    """
    path = os.path.join(base_path, day, "scored_enriched.jsonl")
    if not os.path.exists(path):
        raise FileNotFoundError(f"[utils] Could not find scored_enriched file at {path}")

    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return items


def get_persona_block(role: str, pod: str):
    """
    Wrapper som hämtar persona_id och persona_block för en given roll och pod.
    Alla sektioner kan fortsätta ropa på utils.get_persona_block.
    """
    pod_cfg = role_utils.get_pod_config(pod)
    persona_id = role_utils.resolve_persona_for_role(pod_cfg, role)
    persona_block = role_utils.resolve_block_for_persona(persona_id)
    return persona_id, persona_block
