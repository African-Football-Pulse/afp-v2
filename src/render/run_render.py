import os
import datetime
import pathlib
import sys
import json

from src.common.blob_io import get_container_client
from . import tts_elevenlabs


def log(msg: str) -> None:
    print(f"[render] {msg}", flush=True)


def load_manifest(manifest_path: pathlib.Path) -> dict:
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def main():
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "premier_league")

    # Normalisera språk: använd "en" om LANG saknas eller är typ "C.UTF-8"
    _raw_lang = os.getenv("LANG")
    lang = _raw_lang if _raw_lang and not _raw_lang.startswith("C.") else "en"

    log(f"Start render: date={date}, league={league}, lang={lang}")

    # Init blob container
    container = get_container_client()

    # Blob paths
    base_in = f"assembler/episodes/{date}/{league}/daily/{lang}/"
    base_out = f"audio/episodes/{date}/{league}/daily/{lang}/"

    # Lokala paths
    local_in = pathlib.Path(base_in)
    local_out = pathlib.Path(base_out)
    local_in.mkdir(parents=True, exist_ok=True)
    local_out.mkdir(parents=True, exist_ok=True)

    # 1) Hämta manifest från blob
    fname = "episode_manifest.json"
    blob_name = base_in + fname
    log(f"Laddar ner {blob_name}")
    blob = container.get_blob_client(blob=blob_name)
    data = blob.download_blob().readall().decode("utf-8")
    (local_in / fname).write_text(data, encoding="utf-8")

    # 2) Läs manifest
    manifest = load_manifest(local_in / "episode_manifest.json")
    sections = {s["section_id"]: s for s in manifest.get("sections", [])}

    log(f"Manifest-sektioner: {len(sections)}")

    if not sections:
        log("ERROR: Inga sektioner i manifestet.")
        sys.exit(1)

    # 2b) Rensa bort streck '---' ur texterna
    def clean_section_text(section):
        if "text" in section:
            section["text"] = section["text"].replace("---", "").strip()
        if "lines" in section:
            for l in section["lines"]:
                l["text"] = l["text"].replace("---", "").strip()
        return section

    sections = {sid: clean_section_text(sec) for sid, sec in sections.items()}

    # 3) Kör ElevenLabs-rendering med hela section-objekten (text/lines)
    try:
        tts_elevenlabs.main(sections)
    except Exception as e:
        log(f"FEL under TTS: {e}")
        sys.exit(1)

    # 4) Ladda upp resultatfiler
    for fname in ["episode.mp3", "render_manifest.json", "report.json"]:
        path = local_out / fname
        if not path.exists():
            continue
        log(f"Laddar upp {base_out}{fname}")
        blob = container.get_blob_client(blob=base_out + fname)
        content_type = "application/json" if fname.endswith(".json") else "audio/mpeg"
        blob.upload_blob(path.read_bytes(), overwrite=True, content_type=content_type)

    log("Klart ✅")


if __name__ == "__main__":
    main()

