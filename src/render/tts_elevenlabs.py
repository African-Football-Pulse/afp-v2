import os, json, pathlib, datetime, sys, tempfile
import requests
from pydub import AudioSegment  # kräver ffmpeg i workflow

# ---------- helpers ----------
def ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def read_text(p: pathlib.Path) -> str:
    return p.read_text(encoding="utf-8") if p.exists() else ""

def write_json(p: pathlib.Path, data: dict):
    ensure_dir(p)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def clean_persona(name: str) -> str:
    return (name or "").strip().lower().replace(" ", "")

def tts_elevenlabs(text: str, voice_id: str, api_key: str, model_id: str, out_fmt: str) -> bytes:
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {"xi-api-key": api_key, "Accept": "audio/mpeg", "Content-Type": "application/json"}
    payload = {"text": text, "model_id": model_id, "output_format": out_fmt}
    r = requests.post(url, headers=headers, json=payload, timeout=180)
    if r.status_code != 200:
        raise RuntimeError(f"ElevenLabs {r.status_code}: {r.text[:400]}")
    return r.content

# ---------- parsing ----------
def parse_from_manifest(manifest_path: pathlib.Path):
    md = {}
    if manifest_path.exists():
        try:
            md = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            md = {}
    title = md.get("title")
    sections = []
    if isinstance(md.get("sections"), list):
        for s in md["sections"]:
            txt = (s.get("text") or "").strip()
            if not txt:
                continue
            sections.append({"persona": clean_persona(s.get("persona")), "text": txt})
    description = md.get("description")
    language = md.get("language")
    return title, description, language, sections

def parse_script(script_path: pathlib.Path, default_persona: str):
    text = read_text(script_path)
    if not text:
        return []
    secs = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if ":" in line:
            tag, rest = line.split(":", 1)
            persona = clean_persona(tag)
            secs.append({"persona": persona, "text": rest.strip()})
        else:
            secs.append({"persona": clean_persona(default_persona), "text": line})
    return secs

# ---------- main ----------
def main():
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "epl")
    lang = os.getenv("LANG", "en")
    default_persona = clean_persona(os.getenv("DEFAULT_PERSONA") or "ak")

    audio_format = os.getenv("AUDIO_FORMAT", "mp3")
    rate = int(os.getenv("RATE", "22050"))
    model_id = os.getenv("ELEVENLABS_MODEL_ID", "eleven_multilingual_v2")
    out_fmt = f"{audio_format}_{rate}"  # mp3_22050

    api_key = os.getenv("ELEVENLABS_API_KEY", "")
    if not api_key:
        print("ERROR: ELEVENLABS_API_KEY saknas"); sys.exit(1)

    base_in = pathlib.Path(f"assembler/episodes/{date}/{league}/daily/{lang}")
    base_out = pathlib.Path(f"audio/episodes/{date}/{league}/daily/{lang}")
    script_path = base_in / "episode_script.txt"
    manifest_path = base_in / "episode_manifest.json"
    audio_path = base_out / f"episode.{audio_format}"
    render_manifest_path = base_out / "render_manifest.json"
    report_path = base_out / "report.json"

    # ---- voice config: multi först, annars single ----
    voice_map = {}
    multi_voice_secret = os.getenv("ELEVENLABS_VOICE_IDS", "")
    if multi_voice_secret:
        try:
            vm = json.loads(multi_voice_secret)
            if isinstance(vm, dict) and vm:
                voice_map = {clean_persona(k): v for k, v in vm.items() if v}
        except Exception:
            voice_map = {}

    single_voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")

    mode = "multi" if voice_map else "single"
    if mode == "single" and not single_voice_id:
        # ge tydligt fel om varken JSON-map eller single-voice finns
        print("ERROR: Hittar varken ELEVENLABS_VOICE_IDS (JSON) eller ELEVENLABS_VOICE_ID.")
        sys.exit(1)

    # ---- läs manifest/script ----
    title, description, language, manifest_sections = parse_from_manifest(manifest_path)
    if manifest_sections:
        sections = manifest_sections
    else:
        if not script_path.exists():
            print(f"ERROR: Hittar inte manus: {script_path}"); sys.exit(1)
        sections = parse_script(script_path, default_persona)

    if not sections:
        print("ERROR: Inga sektioner att rendera."); sys.exit(1)

    # ---- render ----
    ensure_dir(audio_path)
    if mode == "single":
        # rendera allt i ett svep
        joined = "\n".join(s["text"] for s in sections)
        print(f"Render (single voice) – tecken={len(joined)}")
        audio_bytes = tts_elevenlabs(joined, single_voice_id, api_key, model_id, out_fmt)
        pathlib.Path(audio_path).write_bytes(audio_bytes)
        sections_summary = [{"persona": "single", "chars": len(joined)}]
    else:
        # multi-voice: sektion för sektion
        print(f"Render (multi voice) – sektioner={len(sections)}")
        tmp_files = []
        with tempfile.TemporaryDirectory() as td:
            for i, s in enumerate(sections):
                persona = s["persona"] or default_persona
                voice_id = voice_map.get(persona) or voice_map.get(default_persona)
                if not voice_id:
                    print(f"ERROR: Hittar ingen voice_id för persona '{persona}' och ingen default i ELEVENLABS_VOICE_IDS.")
                    sys.exit(1)
                txt = s["text"]
                print(f"TTS {i+1}/{len(sections)} – persona={persona}, tecken={len(txt)}")
                seg_bytes = tts_elevenlabs(txt, voice_id, api_key, model_id, out_fmt)
                p = pathlib.Path(td) / f"seg_{i:03d}.mp3"
                p.write_bytes(seg_bytes)
                tmp_files.append(p)

            # concat + 300ms paus mellan
            combined = AudioSegment.silent(duration=0)
            pause = AudioSegment.silent(duration=300)
            for i, p in enumerate(tmp_files):
                combined += AudioSegment.from_file(p, format="mp3")
                if i < len(tmp_files) - 1:
                    combined += pause
            combined.export(audio_path, format="mp3", bitrate="128k")
        sections_summary = [{"persona": s["persona"] or default_persona, "chars": len(s["text"])} for s in sections]

    # ---- manifest + rapport ----
    render_manifest = {
        "engine": "elevenlabs",
        "model_id": model_id,
        "mode": mode,
        "date": date,
        "league": league,
        "lang": lang,
        "audio_format": "mp3",
        "sample_rate": rate,
        "title": title or f"{league.upper()} daily – {date} ({lang})",
        "description": description,
        "sections": sections_summary,
    }
    write_json(render_manifest_path, render_manifest)
    write_json(report_path, {
        "status": "ok",
        "audio_path": str(audio_path),
        "render_manifest": str(render_manifest_path),
        "title": render_manifest["title"]
    })
    print("Klart: episode.mp3 skapad.")

if __name__ == "__main__":
    main()
