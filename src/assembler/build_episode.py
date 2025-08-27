import os, json, pathlib, hashlib, sys
from datetime import datetime
from typing import List

# Våra gemensamma helpers
from src.common.blob_io import get_container_client

# ---- Parametrar (kan styras via env)
LEAGUE = os.getenv("LEAGUE", "premier_league")
LANG   = os.getenv("LANG", "en")
POD_ID = os.getenv("POD_ID", f"afp-{LEAGUE}-daily-{LANG}")

USE_LOCAL   = os.getenv("USE_LOCAL", "0") == "1"
LOCAL_ROOT  = pathlib.Path("/app/local_out")

READ_PREFIX  = "" if USE_LOCAL else os.getenv("READ_PREFIX", "producer/")
WRITE_PREFIX = "" if USE_LOCAL else os.getenv("BLOB_PREFIX", os.getenv("WRITE_PREFIX", "assembler/"))

# Sätts i main() om online
_CONTAINER = None  # type: ignore

def log(msg: str) -> None:
    print(f"[assemble] {msg}", flush=True)

def today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def _ensure_parent(p: pathlib.Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

# ---------- I/O helpers ----------
def _read_text_local(rel_path: str) -> str:
    p = LOCAL_ROOT / rel_path
    if not p.exists():
        raise FileNotFoundError(f"Local file not found: {p}")
    return p.read_text(encoding="utf-8")

def _write_text_local(rel_path: str, text: str, content_type: str = "text/plain; charset=utf-8") -> str:
    p = LOCAL_ROOT / rel_path
    _ensure_parent(p)
    p.write_text(text, encoding="utf-8")
    return str(p)

def read_text(rel_path: str) -> str:
    if USE_LOCAL:
        return _read_text_local(rel_path)
    blob = _CONTAINER.get_blob_client(blob=READ_PREFIX + rel_path)  # type: ignore[attr-defined]
    return blob.download_blob().readall().decode("utf-8")

def write_text(rel_path: str, text: str, content_type: str) -> str:
    if USE_LOCAL:
        return _write_text_local(rel_path, text, content_type)
    blob = _CONTAINER.get_blob_client(blob=WRITE_PREFIX + rel_path)  # type: ignore[attr-defined]
    blob.upload_blob(text.encode("utf-8"), overwrite=True, content_type=content_type)
    return WRITE_PREFIX + rel_path

def list_section_manifests(date: str, league: str, lang: str) -> List[str]:
    """
    Returnerar lista av relativa blob-/filvägar som slutar med /section_manifest.json
    under sections/{date}/{league}/_/.../{lang}/section_manifest.json
    """
    base_prefix = f"sections/{date}/{league}/_/"
    results: List[str] = []
    if USE_LOCAL:
        base = LOCAL_ROOT / base_prefix
        if not base.exists():
            return []
        for p in base.rglob("section_manifest.json"):
            posix = p.relative_to(LOCAL_ROOT).as_posix()
            if f"/{lang}/section_manifest.json" in posix:
                results.append(posix)
        return sorted(results)
    else:
        for b in _CONTAINER.list_blobs(name_starts_with=READ_PREFIX + base_prefix):  # type: ignore[attr-defined]
            name = b.name  # type: ignore[attr-defined]
            if name.endswith("/section_manifest.json") and f"/{lang}/" in name:
                # ta bort READ_PREFIX för att få en "relativ" väg i resten av koden
                results.append(name[len(READ_PREFIX):])
        return sorted(results)

# ---------- Domänlogik ----------
def build_episode(date: str, league: str, lang: str):
    manifests = list_section_manifests(date, league, lang)
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
        # Strukturen är .../<SECTION_ID>/<LANG>/section_manifest.json
        parts = m.split("/")
        try:
            section_id = parts[-3]
        except Exception:
            section_id = "UNKNOWN"
        # Läs manifestet för att plocka target_duration_s om finns
        try:
            raw = json.loads(read_text(m))
            dur = int(raw.get("target_duration_s", 60))
        except Exception:
            dur = 60
        sections_meta.append({"section_id": section_id, "lang": lang, "duration_s": dur})
        total += dur

    script = {
        "pod_id": POD_ID,
        "date": date,
        "type": "micro",
        "lang": lang,
        "jingles": {"intro": "jingles/J2.mp3", "outro": "jingles/J2.mp3"},
        "sections": sections_meta,
        "duration_s": total,
    }

    write_text(base + "episode_manifest.json", json.dumps(script, ensure_ascii=False, indent=2), "application/json")
    write_text(base + "episode_script.txt",
               "Micro episode (stub) – stitching in Sprint 3",
               "text/plain; charset=utf-8")
    log(f"wrote: {(WRITE_PREFIX or '[local]/')}{base}episode_manifest.json")
    log(f"wrote: {(WRITE_PREFIX or '[local]/')}{base}episode_script.txt")

def main():
    mode = "LOCAL" if USE_LOCAL else "SAS"
    log(f"start mode={mode} league={LEAGUE} lang={LANG}")
    if not USE_LOCAL:
        # Säkerställer att SAS finns & är giltig
        global _CONTAINER
        _CONTAINER = get_container_client()

    date = today()
    build_episode(date, LEAGUE, LANG)
    log("done")

if __name__ == "__main__":
    main()
