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
    section_code: str,
    date: str,
    news: str | None = None,   # gör optional
    league: str = "_",
    topic: str = "_",
    layout: str = "alias-first",
    path_scope: str = "single",
    write_latest: bool = True,
    dry_run: bool = False,
    outdir: str = "outputs/sections",
    model: str = "static",
    type: str = "stats",
) -> dict:
    """
    Build 'Top African Players' section and write outputs to Azure Blob or local fs.
    """

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    # --- bygg själva texten (placeholder tills riktig logik är på plats)
    text = "No news items available."
    payload = {
        "slug": "top_african_players",
        "title": "Top African Players this week",
        "text": text,
        "length_s": 2,
        "sources": [],
        "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
    }

    # --- paths
    if layout == "alias-first":
        base = f"sections/{section_code}/{date}/{league}/{topic}"
    else:
        base = f"sections/{date}/{league}/{topic}/{section_code}"

    json_rel = f"{base}/section.json"
    md_rel   = f"{base}/section.md"
    man_rel  = f"{base}/section_manifest.json"

    # --- build artifacts
    json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    md_bytes = (f"### Top African Players\n\n{text}\n").encode("utf-8")

    manifest = {
        "section_code": section_code,
        "type": type,
        "model": model,
        "created_utc": ts,
        "league": league,
        "topic": topic,
        "date": date,
        "blobs": {"json": json_rel, "md": md_rel},
        "metrics": {"length_s": payload["length_s"]},
        "sources": {"news_input_path": str(news) if news else None},
    }
    man_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    # --- write to Azure Blob or local
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
