# src/render/run_render.py
import os
import datetime
import pathlib
import sys

# importera blob-helpers från samma plats där assemble hämtar/laddar upp
from lib.blob import download_from_blob, upload_to_blob  
from render import tts_elevenlabs


def main():
    # Inputs (samma miljövariabler som i tts_elevenlabs)
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "epl")
    lang = os.getenv("LANG", "en")

    # Paths
    base_in = f"assembler/episodes/{date}/{league}/daily/{lang}/"
    base_out = f"audio/episodes/{date}/{league}/daily/{lang}/"

    # Lokala kataloger
    local_in = pathlib.Path(base_in)
    local_out = pathlib.Path(base_out)

    # Se till att lokala mappar finns
    local_in.mkdir(parents=True, exist_ok=True)
    local_out.mkdir(parents=True, exist_ok=True)

    # 1) Hämta input från blob (manifest + script från assemble)
    print(f"[render] Hämtar input från blob: {base_in}")
    download_from_blob(base_in, local_in)

    # 2) Kör render (ElevenLabs TTS)
    print(f"[render] Startar TTS render för {date}/{league}/{lang}")
    try:
        tts_elevenlabs.main()
    except Exception as e:
        print(f"[render] FEL under TTS: {e}")
        sys.exit(1)

    # 3) Ladda upp output (audio + manifest + report) till blob
    print(f"[render] Laddar upp resultat till blob: {base_out}")
    upload_from = local_out  # här ligger episode.mp3 + render_manifest.json + report.json
    upload_to_blob(base_out, upload_from)

    print("[render] Klart ✅")


if __name__ == "__main__":
    main()
