import os, json
from datetime import datetime
from typing import List
from jinja2 import Environment, FileSystemLoader
from src.storage import azure_blob
from src.tools.voice_map import load_voice_map
from src.tools import transitions_utils   # 🧩 Övergångar
from src.tools import episode_frame_utils # 🧩 Intro/Outro (stub i nuläget)

# ---------- Miljövariabler ----------
LEAGUE = os.getenv("LEAGUE", "premier_league")
_raw_lang = os.getenv("LANG")
LANG = _raw_lang if _raw_lang and not _raw_lang.startswith("C.") else "en"

# 🔄 Ny: POD matchar "pod" från produce/write_outputs
POD = os.getenv("POD", "PL_daily_africa_en")
POD_ID = os.getenv("POD_ID", f"afp-{LEAGUE}-daily-{LANG}")

READ_PREFIX = os.getenv("READ_PREFIX", "")
WRITE_PREFIX = os.getenv("BLOB_PREFIX", os.getenv("WRITE_PREFIX", "assembler/"))

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
if not CONTAINER.strip():
    raise RuntimeError("AZURE_STORAGE_CONTAINER missing or empty")


# ---------- Logging ----------
def log(msg: str):
    print(f"[assemble] {msg}", flush=True)


def today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


# ---------- I/O ----------
def read_text(rel_path: str) -> str:
    """Läser text från Azure Blob"""
    return azure_blob.get_text(CONTAINER, READ_PREFIX + rel_path)


def write_text(rel_path: str, text: str, content_type: str):
    """Skriver text till Azure Blob"""
    azure_blob.put_text(CONTAINER, WRITE_PREFIX + rel_path, text, content_type)
    return WRITE_PREFIX + rel_path


# ---------- Hitta sektioner ----------
def list_section_manifests(date: str, league: str, pod: str) -> List[str]:
    """Listar alla section_manifest.json för datum/league/pod"""
    base_prefix = f"{READ_PREFIX}sections/"
    results = []
    for b in azure_blob.list_prefix(CONTAINER, base_prefix):
        if b.endswith("/section_manifest.json") and f"/{date}/{league}/{pod}/" in b:
            results.append(b[len(READ_PREFIX):])
    log(f"found {len(results)} manifests for pod={pod}")
    return sorted(results)


# ---------- Parser ----------
def parse_section_text(section_id: str, date: str, league: str, pod: str) -> dict:
    """Läser section.md och returnerar text eller radvis persona-struktur"""
    md_path = f"sections/{section_id}/{date}/{league}/{pod}/section.md"
    try:
        raw_text = read_text(md_path).strip()
    except Exception:
        return {"text": ""}

    clean_lines = [l for l in raw_text.splitlines() if not l.strip().startswith("#")]
    raw_text = "\n".join(clean_lines).strip()

    lines = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("**Expert 1:**") or line.startswith("Expert 1:"):
            lines.append({"persona": "expert1", "text": line.split(":", 1)[1].strip()})
        elif line.startswith("**Expert 2:**") or line.startswith("Expert 2:"):
            lines.append({"persona": "expert2", "text": line.split(":", 1)[1].strip()})

    return {"lines": lines} if lines else {"text": raw_text}


# ---------- Rendering via Jinja ----------
def render_episode(sections_meta, lang: str, mode: str = "script"):
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("episode.jinja")
    sections_dict = {s["section_id"]: s for s in sections_meta}
    return template.render(
        sections=sections_dict,
        weekday=datetime.utcnow().weekday(),
        lang=lang,
        mode=mode,
        league=LEAGUE,
        date=today(),
    )


# ---------- Domänlogik ----------
def build_episode(date: str, league: str, lang: str, pod: str):
    manifests = list_section_manifests(date, league, pod)
    base = f"episodes/{date}/{league}/daily/{lang}/"

    if not manifests:
        report = {
            "status": "no-episode",
            "reason": f"no sections found for pod={pod}",
            "date": date,
            "league": league,
            "lang": lang,
        }
        write_text(base + "report.json", json.dumps(report, ensure_ascii=False, indent=2), "application/json")
        log(f"wrote: {WRITE_PREFIX}{base}report.json")
        return

    sections_meta = []
    for m in manifests:
        parts = m.split("/")
        section_id = parts[1] if len(parts) > 1 else "UNKNOWN"
        try:
            raw = azure_blob.get_json(CONTAINER, m)
            dur = int(raw.get("target_duration_s", 60))
            role = raw.get("role", "news_anchor")
        except Exception:
            dur, role = 60, "news_anchor"

        parsed = parse_section_text(section_id, date, league, pod)
        sections_meta.append({"section_id": section_id, "role": role, "lang": lang, "duration_s": dur, **parsed})

    log(f"[debug] sections_meta count={len(sections_meta)}")
    if sections_meta:
        log(f"[debug] first_section={json.dumps(sections_meta[0], ensure_ascii=False)[:300]}")

    # 🧩 Infoga övergångar
    sections_with_trans = transitions_utils.insert_transitions(sections_meta, lang)
    added_transitions = [s for s in sections_with_trans if s["section_id"].startswith("TRANSITION.")]
    for t in added_transitions:
        log(f"added transition: {t['section_id']} ({t['text'][:60]}...)")

    # 🧩 Infoga intro/outro (stub loggar bara)
    sections_with_frame = episode_frame_utils.insert_intro_outro(sections_with_trans, lang)
    if len(sections_with_frame) != len(sections_with_trans):
        log(f"[frame] intro/outro added → total sections: {len(sections_with_frame)}")

    # 1️⃣ Rendera manus
    episode_script = render_episode(sections_with_frame, lang, mode="script")

    # 2️⃣ Rendera vilka sektioner som används
    used_text = render_episode(sections_with_frame, lang, mode="used")
    used_sections = [line.strip() for line in used_text.splitlines() if line.strip()]
    log(f"used_sections (från mallen): {used_sections}")

    # 3️⃣ Filtrera metadata
    filtered_meta = [s for s in sections_with_frame if s["section_id"] in used_sections]

    # 4️⃣ Voice map
    voice_map = load_voice_map(lang)

    manifest = {
        "pod_id": POD_ID,
        "pod": pod,
        "date": date,
        "type": "micro",
        "lang": lang,
        "sections": filtered_meta,
        "duration_s": sum(s["duration_s"] for s in filtered_meta),
        "voice_map": voice_map,
    }

    # 5️⃣ Skriv resultat till Azure
    write_text(base + "episode_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2), "application/json")
    write_text(base + "episode_script.txt", episode_script, "text/plain; charset=utf-8")
    write_text(base + "episode_used.json", json.dumps(used_sections, ensure_ascii=False, indent=2), "application/json")

    log(f"wrote: {WRITE_PREFIX}{base}episode_manifest.json")
    log(f"wrote: {WRITE_PREFIX}{base}episode_script.txt")
    log(f"wrote: {WRITE_PREFIX}{base}episode_used.json")


# ---------- Main ----------
def main():
    log(f"start assemble: league={LEAGUE} lang={LANG} pod={POD}")
    date = today()
    build_episode(date, LEAGUE, LANG, POD)
    log("done")


if __name__ == "__main__":
    main()
