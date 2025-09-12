# src/assembler/assemble_episode.py
import argparse, json, os, sys, yaml, time
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

def _get_blob_json(container_sas_url: str, blob_path: str):
    """Försöker hämta och returnera JSON från Azure Blob."""
    url = _make_blob_url(container_sas_url, blob_path)
    r = requests.get(url, timeout=30)
    if r.status_code == 200:
        return r.json(), blob_path
    return None, blob_path

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

# ---------- Helpers ----------
def load_yaml(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_section_text(container_sas_url, date, section_code, league, lang="en", topic="_"):
    """
    Läser section.json direkt från Azure Blob via SAS.
    Försöker först utan språk, sedan med språk.
    """
    paths = [
        f"sections/{section_code}/{date}/{league}/{topic}/section.json",
        f"sections/{section_code}/{date}/{league}/{topic}/{lang}/section.json",
    ]
    for blob_path in paths:
        m, used_path = _get_blob_json(container_sas_url, blob_path)
        if m:
            log(f"[INFO] Hittade sektion i Blob: {used_path}")
            return m.get("text", "").strip(), used_path
    log(f"[WARN] Ingen sektion hittades för {section_code} (testade {paths})")
    return None, paths[0]

def today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")

def list_plan_sections(plan_path: str, mode: str):
    """
    Hämtar sektioner från produce_plan.
    Stödjer både:
    - Ny struktur: tasks: [ {section_code: ...}, ... ]
    - Äldre struktur: jobs: [ {mode: ..., sections: [...]}, ... ]
    """
    plan = load_yaml(plan_path)
    sections = []

    if "tasks" in plan:
        # Flat lista, ignorera mode (alla tasks gäller)
        sections = plan["tasks"]
    elif "jobs" in plan:
        for job in plan.get("jobs", []):
            if job.get("mode") == mode:
                sections.extend(job.get("sections", []))

    return sections

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=False, help="YYYY-MM-DD (default = today UTC)")
    ap.add_argument("--league", required=True)
    ap.add_argument("--mode", default="postmatch")
    ap.add_argument("--lang", default="en")
    ap.add_argument("--template", default="templates/episodes/postmatch.yaml")
    ap.add_argument("--plan", default="config/produce_plan.yaml")
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
            text, src_path = read_section_text(sas, args.date, s["section_code"], args.league, args.lang)
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

        elif s["type"] == "dynamic":
            plan_sections = list_plan_sections(args.plan, args.mode)
            log(f"[DEBUG] Plan sections loaded: {[sec.get('section_code') for sec in plan_sections]}")
            for sec in plan_sections:
                code = sec.get("section_code")
                if not code:
                    continue
                text, src_path = read_section_text(sas, args.date, code, args.league, args.lang)
                if text:
                    persona = sec.get("persona", s.get("persona", "AK"))
                    lines.append(f"[{persona}] {text}")
                    manifest_segments.append({
                        "type": "section",
                        "section_code": code,
                        "persona": persona,
                        "source": src_path
                    })
                else:
                    manifest_segments.append({
                        "type": "section",
                        "section_code": code,
                        "persona": sec.get("persona", s.get("persona", "AK")),
                        "source": src_path,
                        "missing": True
                    })

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
