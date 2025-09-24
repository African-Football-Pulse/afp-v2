import os, json, pathlib
from datetime import datetime
from typing import List
from src.common.blob_io import get_container_client
from jinja2 import Environment, FileSystemLoader
from src.tools.voice_map import load_voice_map   # <-- nytt

LEAGUE = os.getenv("LEAGUE", "premier_league")

_raw_lang = os.getenv("LANG")
LANG = _raw_lang if _raw_lang and not _raw_lang.startswith("C.") else "en"

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

# ---------- Parser ----------
def parse_section_text(section_id: str, date: str, league: str) -> dict:
    """
    Läser section.md och returnerar {"text": "..."} eller {"lines": [{"persona": "...", "text": "..."}]}
    """
    md_path = f"sections/{section_id}/{date}/{league}/_/section.md"
    try:
        raw_text = read_text(md_path).strip()
    except Exception:
        return {"text": ""}

    lines = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("**Expert 1:**") or line.startswith("Expert 1:"):
            text = line.split(":", 1)[1].strip()
            lines.append({"persona": "expert1", "text": text})
        elif line.startswith("**Expert 2:**") or line.startswith("Expert 2:"):
            text = line.split(":", 1)[1].strip()
            lines.append({"persona": "expert2", "text": text})

    if lines:
        return {"lines": lines}
    else:
        return {"text": raw_text}

# ---------- Rendering via Jinja ----------
def render_episode(sections_meta, lang: str) -> str:
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("episode.jinja")
    sections_dict = {s["section_id"]: s for s in sections_meta}
    return template.render(
        sections=sections_dict,
        weekday=datetime.utcnow().weekday(),
        lang=lang
    )

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

        parsed = parse_section_text(section_id, date, league)

        sections_meta.append({
            "section_id": section_id,
            "lang": lang,
            "duration_s": dur,
            **parsed
        })
        total += dur

    # Ladda voice_map
    voice_map = load_voice_map(lang)

    manifest = {
        "pod_id": POD_ID,
        "date": date,
        "type": "micro",
        "lang": lang,
        "sections": sections_meta,
        "duration_s": total,
        "voice_map": voice_map,   # <-- nytt
    }

    write_text(base + "episode_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2), "application/json")

    # Bygg script via Jinja-mall
    episode_script = render_episode(sections_meta, lang)
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
