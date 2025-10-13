#!/usr/bin/env python3
"""
Assemble – bygger en komplett episod utifrån producerade sektioner.

Nyheter:
 - Inline GPT-förädling av varje sektion (ingen separat modul)
 - Filtrering på rating ≥ RATING_THRESHOLD
 - Deduplicering av textinnehåll
 - Dynamisk rendering baserad på weekday & rating
"""

import os, json, hashlib
from datetime import datetime
from typing import List
from jinja2 import Environment, FileSystemLoader
from openai import OpenAI

from src.storage import azure_blob
from src.tools.voice_map import load_voice_map
from src.tools import transitions_utils
from src.tools import episode_frame_utils


# ---------- Miljövariabler ----------
LEAGUE = os.getenv("LEAGUE", "premier_league")
_raw_lang = os.getenv("LANG")
LANG = _raw_lang if _raw_lang and not _raw_lang.startswith("C.") else "en"

POD = os.getenv("POD", "PL_daily_africa_en")
POD_ID = os.getenv("POD_ID", f"afp-{LEAGUE}-daily-{LANG}")

READ_PREFIX = os.getenv("READ_PREFIX", "")
WRITE_PREFIX = os.getenv("BLOB_PREFIX", os.getenv("WRITE_PREFIX", "assembler/"))

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
if not CONTAINER.strip():
    raise RuntimeError("AZURE_STORAGE_CONTAINER missing or empty")

RATING_THRESHOLD = int(os.getenv("RATING_THRESHOLD", 27))
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-5-turbo")
client = OpenAI()


# ---------- Hjälpfunktioner ----------
def log(msg: str):
    print(f"[assemble] {msg}", flush=True)


def today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def text_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


# ---------- I/O ----------
def read_text(rel_path: str) -> str:
    return azure_blob.get_text(CONTAINER, READ_PREFIX + rel_path)


def write_text(rel_path: str, text: str, content_type: str):
    azure_blob.put_text(CONTAINER, WRITE_PREFIX + rel_path, text, content_type)
    return WRITE_PREFIX + rel_path


# ---------- Hitta sektioner ----------
def list_section_manifests(date: str, league: str, pod: str) -> List[str]:
    base_prefix = f"{READ_PREFIX}sections/"
    results = []
    for b in azure_blob.list_prefix(CONTAINER, base_prefix):
        if b.endswith("/section_manifest.json") and f"/{date}/{league}/{pod}/" in b:
            results.append(b[len(READ_PREFIX):])
    log(f"found {len(results)} manifests for pod={pod}")
    return sorted(results)


# ---------- Läs & förädla sektioner ----------
def load_and_refine_section(section_id: str, date: str, league: str, pod: str, lang: str) -> dict:
    md_path = f"sections/{section_id}/{date}/{league}/{pod}/section.md"
    try:
        raw_text = read_text(md_path).strip()
    except Exception:
        log(f"[warn] Missing section text for {section_id}")
        return {"text": "", "original_text": ""}

    # 🧹 Rensa markdown
    clean_lines = [l for l in raw_text.splitlines() if not l.strip().startswith("#")]
    raw_text = "\n".join(clean_lines).strip()

    # 🧠 GPT-förädling inline
    improved_text = raw_text
    try:
        prompt = f"""
        Improve the following podcast section text for clarity, spoken tone, and narrative flow in {lang}.
        Keep meaning, facts, and structure intact. Do not shorten drastically.
        Section ID: {section_id}.
        ---
        {raw_text}
        """
        response = client.chat.completions.create(
            model=GPT_MODEL,
            messages=[
                {"role": "system", "content": "You are an editorial assistant improving podcast script text."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.7,
            max_tokens=800,
        )
        improved_text = response.choices[0].message.content.strip()
        log(f"[gpt] refined section {section_id}")
    except Exception as e:
        log(f"[gpt] ⚠️ Skipping GPT refine for {section_id}: {e}")

    # 🎭 Identifiera personas (Expert 1, Expert 2 osv.)
    lines = []
    for line in improved_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith(("expert 1:", "**expert 1:**")):
            lines.append({"persona": "expert1", "text": line.split(":", 1)[1].strip()})
        elif line.lower().startswith(("expert 2:", "**expert 2:**")):
            lines.append({"persona": "expert2", "text": line.split(":", 1)[1].strip()})

    return {
        "text": improved_text if not lines else "",
        "lines": lines,
        "original_text": raw_text,
    }


# ---------- Rendering ----------
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


# ---------- Huvudfunktion ----------
def build_episode(date: str, league: str, lang: str, pod: str):
    log(f"🏗️ start assemble: league={league} lang={lang} pod={pod}")

    manifests = list_section_manifests(date, league, pod)
    base = f"episodes/{date}/{league}/daily/{lang}/"

    if not manifests:
        report = {"status": "no-episode", "reason": "no sections found", "date": date}
        write_text(base + "report.json", json.dumps(report, ensure_ascii=False, indent=2), "application/json")
        log("no manifests found – aborting assemble")
        return

    sections_meta = []
    for m in manifests:
        parts = m.split("/")
        section_id = parts[1] if len(parts) > 1 else "UNKNOWN"
        try:
            raw = azure_blob.get_json(CONTAINER, m)
            dur = int(raw.get("target_duration_s", 60))
            role = raw.get("role", "news_anchor")
            rating = int(raw.get("rating", 30))
            source = raw.get("source", "unknown")
        except Exception as e:
            log(f"[warn] failed to read manifest {m}: {e}")
            dur, role, rating, source = 60, "news_anchor", 30, "unknown"

        parsed = load_and_refine_section(section_id, date, league, pod, lang)
        sections_meta.append({
            "section_id": section_id,
            "role": role,
            "lang": lang,
            "duration_s": dur,
            "rating": rating,
            "source": source,
            **parsed
        })

    log(f"🔍 total sections before filter: {len(sections_meta)}")

    # 🔎 Filtrera på rating
    sections_meta = [s for s in sections_meta if s.get("rating", 0) >= RATING_THRESHOLD]
    log(f"✅ kept {len(sections_meta)} sections with rating ≥ {RATING_THRESHOLD}")

    if not sections_meta:
        report = {"status": "no-episode", "reason": "no high-rated sections", "date": date}
        write_text(base + "report.json", json.dumps(report, ensure_ascii=False, indent=2), "application/json")
        log("no eligible sections – aborting assemble")
        return

    # 🚫 Deduplicering
    seen_hashes = set()
    unique_sections = []
    for s in sections_meta:
        h = text_hash(s.get("text", s.get("original_text", "")))
        if h not in seen_hashes:
            seen_hashes.add(h)
            unique_sections.append(s)
        else:
            log(f"[dedupe] skipped duplicate section {s['section_id']}")
    sections_meta = unique_sections

    # 🧩 Infoga övergångar och intro/outro
    with_transitions = transitions_utils.insert_transitions(sections_meta, lang)
    final_sections = episode_frame_utils.insert_intro_outro(with_transitions, lang)

    # 🎙️ Voice map
    voice_map = load_voice_map(lang)

    # 🪄 Rendera manus
    episode_script = render_episode(final_sections, lang, mode="script")
    used_text = render_episode(final_sections, lang, mode="used")
    used_sections = [line.strip() for line in used_text.splitlines() if line.strip()]

    # 🧾 Manifest
    manifest = {
        "pod_id": POD_ID,
        "pod": pod,
        "date": date,
        "type": "micro",
        "lang": lang,
        "sections": final_sections,
        "duration_s": sum(s["duration_s"] for s in final_sections),
        "voice_map": voice_map,
        "weekday": datetime.utcnow().weekday(),
    }

    # 💾 Skriv ut
    write_text(base + "episode_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2), "application/json")
    write_text(base + "episode_script.txt", episode_script, "text/plain; charset=utf-8")
    write_text(base + "episode_used.json", json.dumps(used_sections, ensure_ascii=False, indent=2), "application/json")

    log(f"🎉 Episode built successfully with {len(final_sections)} sections.")
    log(f"Files written under: {WRITE_PREFIX}{base}")


# ---------- Entry ----------
def main():
    date = today()
    build_episode(date, LEAGUE, LANG, POD)
    log("done ✅")


if __name__ == "__main__":
    main()
