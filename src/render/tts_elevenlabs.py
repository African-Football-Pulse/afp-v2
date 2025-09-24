# src/render/tts_elevenlabs.py
import os, json, pathlib, datetime, sys, tempfile
import requests
from pydub import AudioSegment  # kräver ffmpeg i workflow

def ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def write_json(p: pathlib.Path, data: dict):
    ensure_dir(p)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def clean_persona(name: str) -> str:
    return (name or "").strip().lower().replace(" ", "")

def log(msg: str):
    print(msg, flush=True)

def tts_elevenlabs(text: str, voice_id: str, api_key: str, model_id: str, out_fmt: str) -> bytes:
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {
        "xi-api-key": api_key,
        "Accept": "audio/mpeg" if out_fmt.startswith("mp3") else "audio/wav",
        "Content-Type": "application/json",
    }
    payload = {"text": text, "model_id": model_id, "output_format": out_fmt}
    r = requests.post(url, headers=headers, json=payload, timeout=180)
    if r.status_code != 200:
        raise RuntimeError(f"ElevenLabs {r.status_code}: {r.text[:400]}")
    return r.content

def load_voice_map() -> dict:
    vm_env = os.getenv("ELEVENLABS_VOICE_IDS", "")
    if vm_env:
        try:
            vm = json.loads(vm_env)
            if isinstance(vm, dict) and vm:
                return {clean_persona(k): v for k, v in vm.items() if v}
        except Exception:
            pass

    cfg_path = pathlib.Path("config/voice_ids.json")
    if cfg_path.exists():
        try:
            vm = json.loads(cfg_path.read_text(encoding="utf-8"))
            if isinstance(vm, dict) and vm:
                return {clean_persona(k): v for k, v in vm.items() if v}
        except Exception:
            pass
    return {}

def main(section_texts: dict):
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "premier_league")
    lang = os.getenv("LANG") or "en"
    default_persona = clean_persona(os.getenv("DEFAULT_PERSONA") or "ak")

    audio_format = os.getenv("AUDIO_FORMAT", "mp3")
    rate = int(os.getenv("RATE", "22050"))
    model_id = os.getenv("ELEVENLABS_MODEL_ID", "eleven_multilingual_v2")
    out_fmt = f"{audio_format}_{rate}"

    api_key = os.getenv("ELEVENLABS_API_KEY", "")
    if not api_key:
        log("ERROR: ELEVENLABS_API_KEY saknas")
        sys.exit(1)

    base_out = pathlib.Path(f"audio/episodes/{date}/{league}/daily/{lang}")
    audio_path = base_out / f"episode.{audio_format}"
    render_manifest_path = base_out / "render_manifest.json"
    report_path = base_out / "report.json"

    voice_map = load_voice_map()
    single_voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
    mode = "multi" if voice_map else "single"

    if mode == "single" and not single_voice_id:
        log("ERROR: Ingen röst konfigurerad (ELEVENLABS_VOICE_IDS eller ELEVENLABS_VOICE_ID).")
        sys.exit(1)

    # Bygg lista av sektioner
    sections = []
    for sid, sdata in section_texts.items():
        if "lines" in sdata:
            for l in sdata["lines"]:
                persona = clean_persona(l["persona"] or default_persona)
                sections.append({"id": sid, "persona": persona, "text": l["text"]})
        else:
            txt = sdata.get("text", "")
            if txt:
                sections.append({"id": sid, "persona": default_persona, "text": txt})

    if not sections:
        log("ERROR: Inga sektioner att rendera.")
        sys.exit(1)

    ensure_dir(audio_path)
    if mode == "single":
        joined = "\n".join(s["text"] for s in sections)
        log(f"Render (single voice) – tecken={len(joined)}")
        audio_bytes = tts_elevenlabs(joined, single_voice_id, api_key, model_id, out_fmt)
        pathlib.Path(audio_path).write_bytes(audio_bytes)
        sections_summary = [{"persona": "single", "chars": len(joined)}]
    else:
        log(f"Render (multi voice) – sektioner={len(sections)}; röster={list(voice_map.keys())}")
        tmp_files = []
        with tempfile.TemporaryDirectory() as td:
            for i, s in enumerate(sections):
                persona = clean_persona(s["persona"] or default_persona)
                voice_id = voice_map.get(persona) or voice_map.get(default_persona)
                if not voice_id:
                    log(f"ERROR: Ingen voice_id för persona '{persona}' och ingen default.")
                    sys.exit(1)
                txt = s["text"]
                log(f"TTS {i+1}/{len(sections)} – id={s['id']} persona={persona}, tecken={len(txt)}")
                seg_bytes = tts_elevenlabs(txt, voice_id, api_key, model_id, out_fmt)
                p = pathlib.Path(td) / f"seg_{i:03d}.mp3"
                p.write_bytes(seg_bytes)
                tmp_files.append(p)

            combined = AudioSegment.silent(duration=0)
            pause = AudioSegment.silent(duration=300)
            for i, p in enumerate(tmp_files):
                combined += AudioSegment.from_file(p, format="mp3")
                if i < len(tmp_files) - 1:
                    combined += pause
            combined.export(audio_path, format="mp3", bitrate="128k")

        sections_summary = [
            {"id": s["id"], "persona": s["persona"], "chars": len(s["text"])}
            for s in sections
        ]

    render_manifest = {
        "engine": "elevenlabs",
        "model_id": model_id,
        "mode": mode,
        "date": date,
        "league": league,
        "lang": lang,
        "audio_format": audio_format,
        "sample_rate": rate,
        "sections": sections_summary,
    }
    write_json(render_manifest_path, render_manifest)
    write_json(report_path, {
        "status": "ok",
        "audio_path": str(audio_path),
        "render_manifest": str(render_manifest_path)
    })
    log("Klart: episode.mp3 skapad.")
