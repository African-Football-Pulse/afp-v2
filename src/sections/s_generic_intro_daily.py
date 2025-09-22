import os, json, time
from datetime import datetime, UTC
from pathlib import Path
import urllib.parse as up
import requests

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
    """Produce a daily intro section"""
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    # Format date
    try:
        dt = datetime.strptime(args.date, "%Y-%m-%d")
        date_str = dt.strftime("%B %d, %Y")
    except Exception:
        date_str = args.date

    text = (
        f"Welcome to African Football Pulse! "
        f"It’s {date_str}, and this is your daily Premier League update. "
        f"We’ll bring you the latest headlines, key talking points, "
        f"and stories that matter most to African fans."
    )

    # Paths
    base = f"sections/{args.section}/{args.date}/{args.league or '_'}"
    json_rel = f"{base}/section.json"
    md_rel   = f"{base}/section.md"
    man_rel  = f"{base}/section_manifest.json"

    data_payload = {
        "text": text,
        "words_total": len(text.split()),
        "duration_sec_est": int(round(len(text.split()) / 2.6)),
    }
    json_bytes = json.dumps(data_payload, indent=2).encode("utf-8")
    md_bytes   = f"### Daily Intro\n\n{text}\n".encode("utf-8")

    manifest = {
        "section": args.section,
        "type": "generic",
        "model": "static",
        "created_utc": ts,
        "league": args.league,
        "date": args.date,
        "blobs": {"json": json_rel, "md": md_rel},
        "metrics": data_payload,
        "sources": {},
    }
    man_bytes = json.dumps(manifest, indent=2).encode("utf-8")

    sas = os.getenv("BLOB_CONTAINER_SAS_URL") or os.getenv("AFP_AZURE_SAS_URL")
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
