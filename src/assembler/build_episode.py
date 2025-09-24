import os, json, pathlib
from datetime import datetime
from typing import List
from src.common.blob_io import get_container_client

LEAGUE = os.getenv("LEAGUE", "premier_league")
LANG   = os.getenv("LANG", "en")  # Behövs ej för path längre, men kvar i manifest
POD_ID = os.getenv("POD_ID", f"afp-{LEAGUE}-daily-{LANG}")

USE_LOCAL   = os.getenv("USE_LOCAL", "0") == "1"
LOCAL_ROOT  = pathlib.Path("/app/local_out")

READ_PREFIX  = "" if USE_LOCAL else os.getenv("READ_PREFIX", "")
WRITE_PREFIX = "" if USE_LOCAL else os.getenv("BLOB_PREFIX", os.getenv("WRITE_PREFIX", "assembler/"))

_CONTAINER = None  # sätts i main()

def log(msg: str) -> None:
    print(f"[assemble] {msg}", flush=True)

def today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def _ensure_parent(p: pathlib.Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

# ---------- I/O ----------
def read_text(rel_path: str) -> str:
    if USE_LOCAL:
        p = LOCAL_ROOT / rel_path
        return p.read_text(encoding="utf-8")
    blob = _CONTAINER.get_blob_client(blob=READ_PREFIX + rel_path)  # type: ignore[attr-defined]
    return blob.download_blob().readall().decode("utf-8")

def write_text(rel_path: str, text: str, content_type: str) -> str:
    if USE_LOCAL:
        p = LOCAL_ROOT / rel_path
        _ensure_parent(p)
        p.write_text(text, encoding="utf-8")
        return str(p)
    blob = _CONTAINER.get_blob_client(blob=WRITE_PREFIX + rel_path)  # type: ignore[attr-defined]
    blob.upload_blob(text.encode("utf-8"), overwrite=True, content_type=content_type)
    return WRITE_PREFIX + rel_path

def list_section_manifests(date: str, league: str) -> List[str]:
    base_prefix = f"sections/"
    results: List[str] = []
    if USE_LOCAL:
        base = LOCAL_ROOT / base_prefix
        if not base.exists():
            return []
        for p in base.rglob("section_manifest.json"):
            posix = p.relative_to(LOCAL_ROOT).as_posix()
            if f"/{date}/{league}/_/section_manifest.json" in posix:
                results.append(posix)
        return sorted(results)
    else:
        log(f"DEBUG: listing blobs with prefix={READ_PREFIX + base_prefix}")
        for b in _CONTAINER.list_blobs(name_starts_with=READ_PREFIX + base_prefix):  # type: ignore[attr-defined]
            name = b.name  # type: ignore[attr-defined]
            if name.endswith("/section_manifest.json") and f"/{date}/{league}/_/" in name:
                log(f"DEBUG: manifest match -> {name}")
                results.append(name[len(READ_PREFIX):])
        return sorted(results)

# ---------- Domänlogik ----------
def build_episode(date: str, league: str, lang: str):
    manifests = list_section_manifests(date, league)
    log(f"found manifests: {len(manifests)}")
    base = f"episodes/{date}/{league}/daily/{lang}/"

    if not manifests:
        report = {"status": "no-episode", "reason": "no sections found", "date": date, "league": league, "lang": lang}
        write_text(base + "report.json", json.dumps(report, ensure_ascii=False, indent=2), "application/json")
        log(f"wrote: {(WRITE_PREFIX or '[local]/')}{base}report.json")
        return

    sections_meta = []
    total = 0
    for m in manifests:
        parts = m.split("/")
        section_id = parts[1] if len(parts) > 1 else "UNKNOWN"
        try:
            raw = json.loads(read_text(m))
            dur = int(raw.get("target_duration_s", 60))
        except Exception:
            dur = 60

        # Läs in texten från motsvarande section.md
        md_path = f"sections/{section_id}/{date}/{league}/_/section.md"
        try:
            text = read_text(md_path).strip()
        except Exception:
            text = ""

        sections_meta.append({
            "section_id": section_id,
            "lang": lang,
            "duration_s": dur,
            "text": text
        })
        total += dur

    # Nytt manifest: innehåller text i varje sektion
    manifest = {
        "pod_id": POD_ID,
        "date": date,
        "type": "micro",
        "lang": lang,
        "sections": sections_meta,
        "duration_s": total,
    }

    write_text(base + "episode_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2), "application/json")

    # Skapa enkel episode_script.txt för debug/läsning (ingen jingel)
    episode_script = "\n\n---\n\n".join(s["text"] for s in sections_meta if s["text"])
    write_text(base + "episode_script.txt", episode_script, "text/plain; charset=utf-8")

    log(f"wrote: {(WRITE_PREFIX or '[local]/')}{base}episode_manifest.json")
    log(f"wrote: {(WRITE_PREFIX or '[local]/')}{base}episode_script.txt")

def main():
    mode = "LOCAL" if USE_LOCAL else "SAS"
    log(f"start mode={mode} league={LEAGUE} lang={LANG}")
    if not USE_LOCAL:
        global _CONTAINER
        _CONTAINER = get_container_client()
    date = today()
    build_episode(date, LEAGUE, LANG)
    log("done")

if __name__ == "__main__":
    main()
