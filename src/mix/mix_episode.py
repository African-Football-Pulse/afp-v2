import os
import subprocess
from datetime import datetime
from azure.storage.blob import ContainerClient

# Miljövariabler från GitHub Actions / Docker
BLOB_CONTAINER_SAS_URL = os.environ["BLOB_CONTAINER_SAS_URL"]
LEAGUE = os.environ.get("LEAGUE", "premier_league")
_raw_lang = os.getenv("LANG")
LANG = _raw_lang if _raw_lang and not _raw_lang.startswith("C.") else "en"


# Lokala filvägar
INTRO = "assets/audio/afp_intro.mp3"
OUTRO = "assets/audio/afp_outro.mp3"
EPISODE_FILE = "episode.mp3"
FINAL_FILE = "final_episode.mp3"

def log(msg: str):
    print(f"[MIX] {msg}", flush=True)

def get_container_client():
    # Anslut direkt till container med SAS URL
    return ContainerClient.from_container_url(BLOB_CONTAINER_SAS_URL)

def download_episode(container_client, date_str: str):
    blob_path = f"audio/episodes/{date_str}/{LEAGUE}/daily/{LANG}/{EPISODE_FILE}"
    log(f"Laddar ner {blob_path} ...")
    blob_client = container_client.get_blob_client(blob_path)
    with open(EPISODE_FILE, "wb") as f:
        f.write(blob_client.download_blob().readall())
    size = os.path.getsize(EPISODE_FILE)
    log(f"Nedladdning klar. Filstorlek: {size} bytes")

def upload_final(container_client, date_str: str):
    blob_path = f"audio/episodes/{date_str}/{LEAGUE}/daily/{LANG}/{FINAL_FILE}"
    log(f"Laddar upp {FINAL_FILE} till {blob_path} ...")
    blob_client = container_client.get_blob_client(blob_path)
    with open(FINAL_FILE, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    log("Uppladdning klar.")

def mix_files():
    log("Startar mixning ...")
    concat_list = "concat_list.txt"
    with open(concat_list, "w") as f:
        f.write(f"file '{INTRO}'\n")
        f.write(f"file '{EPISODE_FILE}'\n")
        f.write(f"file '{OUTRO}'\n")

    # Debug: logga concat_list och filstorlekar
    with open(concat_list, "r") as f:
        log("Concat list:\n" + f.read())

    for file in [INTRO, EPISODE_FILE, OUTRO]:
        if os.path.exists(file):
            log(f"{file} finns, storlek {os.path.getsize(file)} bytes")
        else:
            log(f"❌ {file} saknas!")

    # Kör ffmpeg med re-encode istället för copy
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", concat_list,
        "-c:a", "libmp3lame", "-b:a", "192k", FINAL_FILE
    ]
    subprocess.run(cmd, check=True)
    log("Mixning klar.")

def main():
    date_str = datetime.today().strftime("%Y-%m-%d")
    container_client = get_container_client()

    download_episode(container_client, date_str)
    mix_files()
    upload_final(container_client, date_str)

if __name__ == "__main__":
    main()
