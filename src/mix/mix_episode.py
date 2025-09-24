import os
import subprocess
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Miljövariabler från GitHub Actions / Docker
BLOB_CONTAINER_SAS_URL = os.environ["BLOB_CONTAINER_SAS_URL"]
LEAGUE = os.environ.get("LEAGUE", "premier_league")
LANG = os.environ.get("LANG", "en")

# Lokala filvägar
INTRO = "assets/audio/afp_intro.mp3"
OUTRO = "assets/audio/afp_outro.mp3"
EPISODE_FILE = "episode.mp3"
FINAL_FILE = "final_episode.mp3"

def log(msg: str):
    print(f"[MIX] {msg}", flush=True)

def get_blob_client():
    # Anslut till blob storage med SAS URL
    return BlobServiceClient(account_url=BLOB_CONTAINER_SAS_URL).get_container_client("afp")

def download_episode(container_client, date_str: str):
    # Full sökväg i blob: afp/audio/episodes/YYYY-MM-DD/league/daily/lang/episode.mp3
    blob_path = f"afp/audio/episodes/{date_str}/{LEAGUE}/daily/{LANG}/{EPISODE_FILE}"
    log(f"Laddar ner {blob_path} ...")
    blob_client = container_client.get_blob_client(blob_path)
    with open(EPISODE_FILE, "wb") as f:
        f.write(blob_client.download_blob().readall())
    log("Nedladdning klar.")

def upload_final(container_client, date_str: str):
    # Ladda upp final_episode.mp3 till samma mapp som episode.mp3
    blob_path = f"afp/audio/episodes/{date_str}/{LEAGUE}/daily/{LANG}/{FINAL_FILE}"
    log(f"Laddar upp {FINAL_FILE} till {blob_path} ...")
    blob_client = container_client.get_blob_client(blob_path)
    with open(FINAL_FILE, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    log("Uppladdning klar.")

def mix_files():
    # Skapa concat-lista för ffmpeg
    log("Startar mixning ...")
    concat_list = "concat_list.txt"
    with open(concat_list, "w") as f:
        f.write(f"file '{INTRO}'\n")
        f.write(f"file '{EPISODE_FILE}'\n")
        f.write(f"file '{OUTRO}'\n")

    # Kör ffmpeg för att kombinera filerna
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", concat_list,
        "-c", "copy", FINAL_FILE
    ]
    subprocess.run(cmd, check=True)
    log("Mixning klar.")

def main():
    date_str = datetime.today().strftime("%Y-%m-%d")
    container_client = get_blob_client()

    download_episode(container_client, date_str)
    mix_files()
    upload_final(container_client, date_str)

if __name__ == "__main__":
    main()
