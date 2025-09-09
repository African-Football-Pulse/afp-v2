# src/sections/s_generic_intro_postmatch.py
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

def build_section(
    *,
    section_code: str,        # e.g. "S.GENERIC.INTRO_POSTMATCH"
    date: str,
    league: str = "_",
    topic: str = "_",
    layout: str = "alias-first",
    write_latest: bool = True,
    dry_run: bool = False,
    outdir: str = "outputs/sections",
    model: str = "static",
    type: str = "generic",
) -> dict:
    """
    Producer entrypoint for Postmatch intro.
    Returns manifest dict; writes blobs via SAS if present, else locally.
    """

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    today_str = datetime.now(UTC).strftime("%B %d, %Y")
    text = (
        f"Welcome to African Football Pulse! "
        f"It’s {today_str}, and we’re coming off a full round of Premier League action. "
        f"Stay tuned as we bring you the biggest results, standout performances, "
        f"and stories that matter most to fans across Africa."
    )

    # Paths
    if layout == "alias-first":
        base = f"sections/{section_code}/{date}/{league}/{topic}"
    else:
        base = f"sections/{date}/{league}/{topic}/{section_code}"

    json_rel = f"{base}/section.json"
    md_rel   = f"{base}/section.md"
    man_rel  = f"{base}/section_manifest.json"

    # Build payloads
    data = {
        "text": text,
        "words_total": len(text.split()),
        "duration_sec_est": int(round(len(text.split()) / 2.6)),  # ~2.6 wps
    }
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    md_lines = [f"### Postmatch Intro", "", text, ""]
    md_bytes = "\n".join(md_lines).encode("utf-8")

    manifest = {
        "section_code": section_code,
        "type": type,
        "model": model,
        "created_utc": ts,
        "league": league,
        "topic": topic,
        "date": date,
        "blobs": {"json": json_rel, "md": md_rel},
        "metrics": {
            "words_total": data["words_total"],
            "duration_sec_est": data["duration_sec_est"],
        },
        "sources": {},
    }
    man_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    # Write (Azure or local)
    sas = os.getenv("BLOB_CONTAINER_SAS_URL") or os.getenv("AFP_AZURE_SAS_URL")
    if sas:
        if dry_run:
            print("=== DRY RUN ===")
            print("Would upload JSON →", _make_blob_url(sas, json_rel))
            print("Would upload MD   →", _make_blob_url(sas, md_rel))
            print("Would upload MAN  →", _make_blob_url(sas, man_rel))
        else:
            _upload_bytes(sas, json_rel, json_bytes, "application/json")
            _upload_bytes(sas, md_rel, md_bytes, "text/markdown")
            _upload_bytes(sas, man_rel, man_bytes, "application/json")
    else:
        outdirp = Path(outdir) / base
        outdirp.mkdir(parents=True, exist_ok=True)
        (outdirp / "section.json").write_text(json_bytes.decode("utf-8"), encoding="utf-8")
        (outdirp / "section.md").write_text(md_bytes.decode("utf-8"), encoding="utf-8")
        (outdirp / "section_manifest.json").write_text(man_bytes.decode("utf-8"), encoding="utf-8")

    return manifest
