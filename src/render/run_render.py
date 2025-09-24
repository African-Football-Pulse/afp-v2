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


def load_script(script_path: pathlib.Path) -> list[str]:
    """Läser in hela episode_script.txt som lista av rader."""
    return script_path.read_text(encoding="utf-8").splitlines()


def extract_sections(manifest: dict, script_lines: list[str]) -> dict:
    """
    Returnerar en mapping {section_id: textblock} baserat på manifestets ordning.
    Här används en enkel strategi: blocken i scriptet följer samma ordning som i manifestet.
    Jinglar ignoreras.
    """
    sections = {}
    buffer = []
    manifest_ids = [s["section_id"] for s in manifest.get("sections", [])]

    current_idx = 0
    current_id = manifest_ids[current_idx] if manifest_ids else None

    for line in script_lines:
        # Hoppa över jinglar (de är gamla rester)
        if line.strip().startswith("[INTRO JINGEL]") or line.strip().startswith("[OUTRO JINGEL]"):
            continue

        # Blockavgränsare = "---"
        if line.strip().startswith("---"):
            if buffer and current_id:
                sections[current_id] = "\n".join(buffer).strip()
                buffer = []
                current_idx += 1
                current_id = manifest_ids[current_idx] if current_idx < len(manifest_ids) else None
            continue

        if current_id:
            buffer.append(line)

    # sista blocket
    if buffer and current_id:
        sections[current_id] = "\n".join(buffer).strip()

    return sections


def main():
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "premier_league")
    lang = os.getenv("LANG") or "en"

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

    # 1) Hämta input-filer (manifest + script) från blob
    for fname in ["episode_manifest.json", "episode_script.txt"]:
        blob_name = base_in + fname
        log(f"Laddar ner {blob_name}")
        blob = container.get_blob_client(blob=blob_name)
        data = blob.download_blob().readall().decode("utf-8")
        (local_in / fname).write_text(data, encoding="utf-8")

    # 2) Läs in manifest och script
    manifest = load_manifest(local_in / "episode_manifest.json")
    script_lines = load_script(local_in / "episode_script.txt")
    section_texts = extract_sections(manifest, script_lines)

    log(f"Script-sektioner i manifest: {len(manifest.get('sections', []))}")
    log(f"Matchade textblock: {len(section_texts)}")

    # 3) Kör ElevenLabs-rendering
    try:
        tts_elevenlabs.main(section_texts)
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
