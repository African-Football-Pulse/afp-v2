import os
import json
from typing import Dict, Any


def write_outputs(
    section_code: str,
    day: str,
    league: str,
    payload: Dict[str, Any],
    lang: str = "en",
    status: str = "success",
    outdir: str = "sections",
) -> Dict[str, Any]:
    """
    Skriver ut payload + manifest till filer och returnerar en wrapper med manifestet.
    """

    if not section_code or not day or not league or payload is None:
        raise ValueError("[write_outputs] Missing required arguments")

    section_path = os.path.join(outdir, section_code, day, league, "_")
    os.makedirs(section_path, exist_ok=True)

    # Manifest
    manifest = {
        "section": section_code,
        "day": day,
        "league": league,
        "status": status,
        "lang": lang,
        "path": section_path,
    }

    # Spara payload (json)
    with open(os.path.join(section_path, "section.json"), "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # Spara manifest
    with open(os.path.join(section_path, "section_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    # Spara markdown-version om text finns
    if "text" in payload:
        with open(os.path.join(section_path, "section.md"), "w") as f:
            f.write(payload["text"])

    print(f"[utils] Uploaded {section_path}/section_manifest.json")

    # 🔑 Viktigt: returnera wrapper som produce_section.py förväntar sig
    return {"manifest": manifest}
