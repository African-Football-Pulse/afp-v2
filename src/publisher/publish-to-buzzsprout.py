import os
import requests
import datetime
import subprocess

# --- Secrets från environment ---
API_KEY = os.environ["BUZZSPROUT_API_KEY"]
PODCAST_ID = os.environ["BUZZSPROUT_PODCAST_ID"]
BLOB_SAS_URL = os.environ["BLOB_SAS_URL"]  # full SAS-URL till containern

# --- Blob-namn (hela sökvägen i containern) ---
BLOB_MP3 = "audio/episodes/2025-09-24/premier_league/daily/en/final_episode.mp3"

# --- Lokala filnamn ---
LOCAL_MP3 = "final_episode.mp3"


def download_from_blob(blob_name: str, local_name: str):
    print(f"⬇️  Hämtar {blob_name} från blob...")

    url_base, sas_token = BLOB_SAS_URL.split("?", 1)
    account_name = url_base.split("//")[1].split(".")[0]   # ex: afpstoragepilot
    container_name = url_base.split(".net/")[1]            # ex: afp

    # Se till att vi skickar med "?" + token
    cmd = [
        "az", "storage", "blob", "download",
        "--account-name", account_name,
        "--container-name", container_name,
        "--name", blob_name,
        "--file", local_name,
        "--sas-token", "?" + sas_token
    ]
    subprocess.check_call(cmd)


# --- 1. Hämta mp3 ---
if not os.path.exists(LOCAL_MP3):
    download_from_blob(BLOB_MP3, LOCAL_MP3)

# --- 2. Sätt default metadata ---
today = datetime.date.today().strftime("%Y-%m-%d")
episode_title = f"Episode {today}"
episode_description = "Automatiskt publicerat av workflow"

# --- 3. Namnge filen lokalt ---
episode_file = f"episode_{today}.mp3"
if LOCAL_MP3 != episode_file:
    os.rename(LOCAL_MP3, episode_file)

# --- 4. Skapa episod i Buzzsprout ---
print(f"📝 Skapar nytt episode i Buzzsprout: {episode_title}")
resp = requests.post(
    f"https://api.buzzsprout.com/api/podcasts/{PODCAST_ID}/episodes.json",
    headers={"Authorization": f"Token token={API_KEY}"},
    json={
        "title": episode_title,
        "description": episode_description,
        "published_at": datetime.datetime.utcnow().isoformat() + "Z"
    }
)
resp.raise_for_status()
episode_id = resp.json()["id"]
print(f"✅ Created episode {episode_id} with title '{episode_title}'")

# --- 5. Ladda upp ljudfilen ---
print(f"⬆️  Laddar upp {episode_file} till Buzzsprout...")
with open(episode_file, "rb") as f:
    resp = requests.patch(
        f"https://api.buzzsprout.com/api/podcasts/{PODCAST_ID}/episodes/{episode_id}.json",
        headers={"Authorization": f"Token token={API_KEY}"},
        files={"audio_file": f}
    )
resp.raise_for_status()
print(f"✅ Uploaded audio file for episode {episode_id}")
