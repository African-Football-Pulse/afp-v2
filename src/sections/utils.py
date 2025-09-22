# src/sections/utils.py
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any


def write_outputs(
    section_id: str,
    day: str,
    league: str,
    payload: Dict[str, Any],
    status: str,
    topic: str = "_",
    lang: str = "en",
) -> Dict[str, Any]:
    """
    Skriv ut section.json, section.md, section_manifest.json
    och returnera manifest.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    base = f"sections/{section_id}/{day}/{league}/{topic}"
    outdir = Path(base)
    outdir.mkdir(parents=True, exist_ok=True)

    # section.json
    (outdir / "section.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # section.md
    title = payload.get("title", section_id)
    text = payload.get("text", "")
    (outdir / "section.md").write_text(f"### {title}\n\n{text}\n", encoding="utf-8")

    # manifest
    manifest = {
        "section_code": section_id,
        "type": payload.get("type", "news"),
        "model": payload.get("model", "gpt-4o-mini"),
        "created_utc": ts,
        "league": league,
        "topic": topic,
        "date": day,
        "blobs": {
            "json": f"{base}/section.json",
            "md": f"{base}/section.md",
        },
        "metrics": {"length_s": payload.get("length_s", 0)},
        "sources": payload.get("sources", {}),
        "lang": lang,
        "status": status,
    }
    (outdir / "section_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return manifest
