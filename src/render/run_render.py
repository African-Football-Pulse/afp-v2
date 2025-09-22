import os
import datetime
import pathlib
import sys

from lib.blob import download_from_blob, upload_to_blob  
from render import tts_elevenlabs


def main():
    # Inputs (samma miljövariabler som i tts_elevenlabs)
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "premier_league")
    lang = os.getenv("LANG", "en")

    # Blob paths
    blob_in = f"afp/assembler/episodes/{date}/{league}/daily/{lang}/"
    blob_out = f"afp/audio/episodes/{date}/{league}/daily/{lang}/"

    # Lokala paths
    local_in = pathlib.Path(f"assembler/episodes/{date}/{league}/daily/{lang}")
    local_out = pathlib.Path(f"audio/episodes/{date}/{league}/daily/{lang}")

    local_in.mkdir(parents=True, exist_ok=True)
    local_out.mkdir(parents=True, exist_ok=True)

    # 1) Hämta input från blob (manifest + script från assemble)
    print(f"[render] Hämtar input från blob: {blob_in}")
    download_from_blob(blob_in, local_in)

    # 2) Kör render (ElevenLabs TTS)
    print(f"[render] Startar TTS render för {date}/{league}/{lang}")
    try:
        tts_elevenlabs.main()
    except Exception as e:
        print(f"[render] FEL under TTS: {e}")
        sys.exit(1)

    # 3) Ladda upp output (audio + manifest + report) till blob
    print(f"[render] Laddar upp resultat till blob: {blob_out}")
    upload_to_blob(blob_out, local_out)

    print("[render] Klart ✅")


if __name__ == "__main__":
    main()
