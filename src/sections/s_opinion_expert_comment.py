import os, json, time
from datetime import datetime, UTC
from pathlib import Path
import urllib.parse as up
import requests

from src.gpt import run_gpt
from src.personas import load_persona

SYSTEM_RULES = """You are an expert scriptwriter for a football podcast.
You must produce a two-speaker dialogue that lasts ~60–75 seconds (≈160–200 words).
Hard constraints:
- Output MUST be in English.
- Stay strictly in character based on the provided persona blocks.
- Fold in news facts without inventing specifics.
- No placeholders like [TEAM]; use only info present in the news input.
- Deliver as a flowing conversation (back-and-forth lines).
- Keep it record-ready: natural pacing, rhetorical devices, light disagreement/banter.
- End with a crisp shared takeaway line.
"""

def _make_blob_url(container_sas_url: str, blob_path: str) -> str:
    p = up.urlparse(container_sas_url)
    base = f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"
    return f"{base}/{blob_path.lstrip('/')}?{p.query}"

def _upload_bytes(container_sas_url: str, blob_path: str, data: bytes,
                  content_type="application/octet-stream", retries=3, backoff=0.8) -> str:
    url = _make_blob_url(container_sas_url, blob_path)
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "x-ms-version": "2020-10-02",
        "Content-Type": content_type,
    }
    for attempt in range(1, retries+1):
        r = requests.put(url, headers=headers, data=data, timeout=60)
        if r.status_code in (201, 202):
            return url
        if attempt == retries:
            raise RuntimeError(f"Blob upload failed ({r.status_code}): {r.text[:500]}")
        time.sleep(backoff * attempt)

def build_section(args) -> dict:
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    persona_ids = args.persona_ids.split(",") if getattr(args, "persona_ids", None) else []
    personas = [load_persona(args.personas, pid) for pid in persona_ids]

    news_path = args.news[0] if args.news else None
    news_items = []
    if news_path and Path(news_path).exists():
        news_items = json.loads(Path(news_path).read_text(encoding="utf-8"))

    prompt = f"Personas:\n{json.dumps(personas, indent=2)}\n\nNews input:\n{json.dumps(news_items[:3], indent=2)}"
    output = run_gpt(SYSTEM_RULES, prompt)

    base = f"sections/{args.section}/{args.date}/{args.league or '_'}"
    json_rel = f"{base}/section.json"
    md_rel   = f"{base}/section.md"
    man_rel  = f"{base}/section_manifest.json"

    data_payload = {"text": output, "personas": persona_ids}
    json_bytes = json.dumps(data_payload, indent=2).encode("utf-8")
    md_bytes   = f"### Duo Experts ({', '.join(p['name'] for p in personas)})\n\n{output}\n".encode("utf-8")

    manifest = {
        "section": args.section,
        "type": "opinion",
        "model": "gpt",
        "created_utc": ts,
        "league": args.league,
        "date": args.date,
        "blobs": {"json": json_rel, "md": md_rel},
        "metrics": {"words": len(output.split())},
        "sources": {"personas": args.personas, "news": news_path},
    }
    man_bytes = json.dumps(manifest, indent=2).encode("utf-8")

    sas = os.getenv("BLOB_CONTAINER_SAS_URL")
    if sas:
        _upload_bytes(sas, json_rel, json_bytes, "application/json")
        _upload_bytes(sas, md_rel, md_bytes, "text/markdown")
        _upload_bytes(sas, man_rel, man_bytes, "application/json")
    else:
        outdirp = Path(args.outdir) / base
        outdirp.mkdir(parents=True, exist_ok=True)
        (outdirp / "section.json").write_bytes(json_bytes)
        (outdirp / "section.md").write_bytes(md_bytes)
        (outdirp / "section_manifest.json").write_bytes(man_bytes)

    return manifest
