# src/assemble/assemble_episode.py
import argparse, json, os, sys, yaml, pathlib, time
from datetime import datetime, UTC
import urllib.parse as up
import requests

def log(msg: str):
    print(f"[assemble] {msg}", flush=True)

# ---------- Azure helpers ----------
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

# ---------- Episode assembly ----------
def load_yaml(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_section_text(base, date, section_code):
    path = pathlib.Path(base) / date / section_code / "manifest.json"
    if not path.exists():
        return None, str(path)
    with open(path, "r", encoding="utf-8") as f:
        m = json.load(f)
    return m.get("payload", {}).get("text", "").strip(), str(path)

def today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=False, help="YYYY-MM-DD (default = today UTC)")
    ap.add_argument("--league", required=True)
    ap.add_argument("--mode", default="postmatch")
    ap.add_argument("--lang", default="en")
    ap.add_argument("--template", default="config/episode_templates/postmatch.yaml")
    ap.add_argument("--sections_root", default="producer/sections")
    args = ap.parse_args()

    if not args.date:
        args.date = today()

    sas = os.getenv("BLOB_CONTAINER_SAS_URL") or os.getenv("AFP_AZURE_SAS_URL")
    if not sas:
        log("ERROR: No BLOB_CONTAINER_SAS_URL set")
        sys.exit(1)

    tpl = load_yaml(args.template)["episode"]
    segs = tpl["segments"]

    lines = []
    manifest_segments = []

    for s in segs:
        if s["type"] == "section":
            text, src_path = read_section_text(args.sections_root, args.date, s["section_code"])
            if text:
                persona = s.get("persona", "AK")
                lines.append(f"[{persona}] {text}")
                manifest_segments.append({
                    "type": "section",
                    "section_code": s["section_code"],
                    "persona": persona,
                    "source": src_path
                })
            else:
                manifest_segments.append({
                    "type": "section",
                    "section_code": s["section_code"],
                    "persona": s.get("persona", "AK"),
                    "source": src_path,
                    "missing": True
                })
                log(f"[WARN] Missing section source: {src_path}")
        else:
            manifest_segments.append(s)

    episode_manifest = {
        "episode_id": f"{args.date}-{args.league}-{args.mode}-{args.lang}",
        "date": args.date,
        "league": args.league,
        "mode": args.mode,
        "lang": args.lang,
        "title": tpl.get("title", "AFP Episode"),
        "segments": manifest_segments,
        "audio": {
            "target_duration_s": 420,
            "tts": {"engine": "elevenlabs", "default_voice": "AK"}
        }
    }

    base = f"assembler/episodes/{args.date}/{args.league}/{args.mode}/{args.lang}/"
    _upload_bytes(sas, base + "episode_manifest.json",
                  json.dumps(episode_manifest, ensure_ascii=False, indent=2).encode("utf-8"),
                  "application/json")
    _upload_bytes(sas, base + "episode_script.txt",
                  "\n\n".join(lines).encode("utf-8"),
                  "text/plain; charset=utf-8")

    log(f"✅ Uploaded episode → {base}")

if __name__ == "__main__":
    sys.exit(main())
