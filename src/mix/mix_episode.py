import os
import pathlib
import subprocess
import json
from datetime import datetime
from src.common.blob_io import get_container_client

# -------------------------------
# Helpers
# -------------------------------
def log(msg: str):
    print(f"[mix] {msg}", flush=True)

def ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def write_json(p: pathlib.Path, data: dict):
    ensure_dir(p)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# -------------------------------
# Main
# -------------------------------
def main():
    # Normalisera datum & språk
    DATE = os.getenv("DATE") or datetime.today().strftime("%Y-%m-%d")
    LEAGUE = os.getenv("LEAGUE", "premier_league")
    _raw_lang = os.getenv("LANG")
    LANG = _raw_lang if _raw_lang and not _raw_lang.startswith("C.") else "en"

    log(f"Start mix: date={DATE}, league={LEAGUE}, lang={LANG}")

    container = get_container_client()

    # Paths
    base_audio = f"audio/episodes/{DATE}/{LEAGUE}/daily/{LANG}/"
    local_tmp = pathlib.Path("/tmp/mix")
    ensure_dir(local_tmp)

    episode_blob = base_audio + "episode.mp3"
    intro_path = pathlib.Path("assets/audio/afp_intro.mp3")
    outro_path = pathlib.Path("assets/audio/afp_outro.mp3")
    local_episode = local_tmp / "episode.mp3"
    final_out = local_tmp / "final_episode.mp3"
    mix_manifest_path = local_tmp / "mix_manifest.json"

    # 1) Hämta episode.mp3 från Blob
    log(f"Laddar ner {episode_blob}")
    blob = container.get_blob_client(blob=episode_blob)
    data = blob.download_blob().readall()
    local_episode.write_bytes(data)

    # 2) Bygg concat-lista
    concat_list = local_tmp / "concat.txt"
    concat_files = []
    with concat_list.open("w", encoding="utf-8") as f:
        if intro_path.exists():
            f.write(f"file '{intro_path.resolve()}'\n")
            concat_files.append(str(intro_path))
        f.write(f"file '{local_episode.resolve()}'\n")
        concat_files.append(str(local_episode))
        if outro_path.exists():
            f.write(f"file '{outro_path.resolve()}'\n")
            concat_files.append(str(outro_path))

    # 3) Kör ffmpeg concat
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(concat_list),
        "-c:a", "libmp3lame", "-b:a", "128k",
        str(final_out)
    ]
    log(f"Kör ffmpeg: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # 4) Ladda upp final + manifest
    final_blob = base_audio + "final_episode.mp3"
    log(f"Laddar upp {final_blob}")
    out_blob = container.get_blob_client(blob=final_blob)
    out_blob.upload_blob(final_out.read_bytes(), overwrite=True, content_type="audio/mpeg")

    manifest = {
        "date": DATE,
        "league": LEAGUE,
        "lang": LANG,
        "inputs": concat_files,
        "output": final_blob,
        "engine": "ffmpeg",
        "codec": "libmp3lame",
        "bitrate": "128k"
    }
    write_json(mix_manifest_path, manifest)

    mix_manifest_blob = base_audio + "mix_manifest.json"
    log(f"Laddar upp {mix_manifest_blob}")
    out_blob = container.get_blob_client(blob=mix_manifest_blob)
    out_blob.upload_blob(mix_manifest_path.read_bytes(), overwrite=True, content_type="application/json")

    log("Klart ✅")


if __name__ == "__main__":
    main()
