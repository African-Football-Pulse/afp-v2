import os, json, pathlib, datetime, sys
import requests

def ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def read_text(p: pathlib.Path) -> str:
    return p.read_text(encoding="utf-8")

def write_bytes(p: pathlib.Path, data: bytes):
    ensure_dir(p)
    p.write_bytes(data)

def write_json(p: pathlib.Path, data: dict):
    ensure_dir(p)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def upload_to_blob_optional(sas_base: str, local_path: pathlib.Path, dest_path: str) -> str | None:
    """
    Om AZURE_SAS_URL (container-SAS) finns: PUT till <sas_base>/<dest_path> + SAS-queryn.
    Exempel: AZURE_SAS_URL="https://acct.blob.core.windows.net/container?<SAS>"
    """
    if not sas_base:
        return None
    if "?" not in sas_base:
        print("WARN: AZURE_SAS_URL saknar SAS-query. Hoppar över blob-upload.")
        return None
    base, sas = sas_base.split("?", 1)
    url = f"{base.rstrip('/')}/{dest_path.lstrip('/')}?{sas}"
    with open(local_path, "rb") as f:
        r = requests.put(url, data=f, headers={"x-ms-blob-type": "BlockBlob"})
    if r.status_code not in (200, 201):
        print(f"WARN: Blob-upload misslyckades: {r.status_code} {r.text[:300]}")
        return None
    return url

def main():
    # Inputs från env (workflow_dispatch kan mappa hit)
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "epl")
    lang = os.getenv("LANG", "en")
    voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")  # t.ex. AK/JJK
    api_key = os.getenv("ELEVENLABS_API_KEY", "")
    audio_format = os.getenv("AUDIO_FORMAT", "mp3")
    sample_rate = int(os.getenv("RATE", "22050"))
    model_id = os.getenv("ELEVENLABS_MODEL_ID", "eleven_multilingual_v2")
    title_override = os.getenv("TITLE_OVERRIDE", "")
    azure_sas = os.getenv("AZURE_SAS_URL", "")  # valfritt

    if not api_key:
        print("ERROR: ELEVENLABS_API_KEY saknas"); sys.exit(1)
    if not voice_id:
        print("ERROR: ELEVENLABS_VOICE_ID saknas"); sys.exit(1)

    base_in = pathlib.Path(f"assembler/episodes/{date}/{league}/daily/{lang}")
    base_out = pathlib.Path(f"audio/episodes/{date}/{league}/daily/{lang}")
    script_path = base_in / "episode_script.txt"
    manifest_in_path = base_in / "episode_manifest.json"
    audio_path = base_out / f"episode.{audio_format}"
    render_manifest_path = base_out / "render_manifest.json"
    report_path = base_out / "report.json"

    if not script_path.exists():
        print(f"ERROR: Hittar inte manus: {script_path}"); sys.exit(1)

    text = read_text(script_path).strip()
    md = {}
    if manifest_in_path.exists():
        try:
            md = json.loads(manifest_in_path.read_text(encoding="utf-8"))
        except Exception:
            md = {}

    # ElevenLabs Text-to-Speech (Create speech)
    # API: POST https://api.elevenlabs.io/v1/text-to-speech/{voice_id}
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {
        "xi-api-key": api_key,
        "Accept": "audio/mpeg",
        "Content-Type": "application/json",
    }
    payload = {
        "text": text,
        "model_id": model_id,
        # Voice settings (valfritt MVP): låt standard gälla
        # "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
        "output_format": f"{audio_format}_{sample_rate}",
    }

    print("Render: skickar manus till ElevenLabs…")
    r = requests.post(url, headers=headers, json=payload, timeout=120)
    if r.status_code != 200:
        print(f"ERROR: ElevenLabs svar {r.status_code}: {r.text[:500]}")
        sys.exit(1)

    audio_bytes = r.content
    write_bytes(audio_path, audio_bytes)

    render_manifest = {
        "engine": "elevenlabs",
        "model_id": model_id,
        "voice_id": voice_id,
        "lang": lang,
        "date": date,
        "league": league,
        "sample_rate": sample_rate,
        "audio_format": audio_format,
        "bytes": len(audio_bytes),
        "source_script": str(script_path),
        "title": title_override or md.get("title") or f"{league.upper()} daily – {date} ({lang})"
    }
    write_json(render_manifest_path, render_manifest)

    # Spara kort rapport
    report = {
        "status": "ok",
        "audio_path": str(audio_path),
        "render_manifest": str(render_manifest_path),
        "title": render_manifest["title"]
    }
    write_json(report_path, report)

    # Valfri blob-upload
    if azure_sas:
        blob_dest_audio = f"{audio_path.as_posix()}"
        blob_dest_manifest = f"{render_manifest_path.as_posix()}"
        uploaded_audio = upload_to_blob_optional(azure_sas, audio_path, blob_dest_audio)
        uploaded_manifest = upload_to_blob_optional(azure_sas, render_manifest_path, blob_dest_manifest)
        if uploaded_audio:
            print(f"Blob upload OK: {uploaded_audio}")
        if uploaded_manifest:
            print(f"Blob upload OK: {uploaded_manifest}")

    print("Klart: episode.mp3 skapad.")

if __name__ == "__main__":
    main()
