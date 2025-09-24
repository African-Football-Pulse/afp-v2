import os
import requests
import datetime
import json
import subprocess

API_KEY = os.environ["BUZZSPROUT_API_KEY"]
PODCAST_ID = os.environ["BUZZSPROUT_PODCAST_ID"]

BLOB_ACCOUNT = os.environ["BLOB_ACCOUNT_NAME"]
BLOB_CONTAINER = os.environ["BLOB_CONTAINER_NAME"]
BLOB_SAS = os.environ["BLOB_CONTAINER_SAS_TOKEN"]

INPUT_MP3 = "final_episode.mp3"
INPUT_JSON = "final_episode.json"


def download_from_blob(filename: str):
    """Ladda ner en blob-fil från Azure Storage till arbetsmappen."""
    print(f"⬇️  Hämtar {filename} från blob...")
    cmd = [
        "az", "storage", "blob", "download",
        "--account-name", BLOB_ACCOUNT,
        "--container-name", BLOB_CONTAINER,
        "--name", filename,
        "--file", filename,
        "--sas-token", BLOB_SAS
    ]
    subprocess.check_call(cmd)


# --- 1. Se till att vi har filerna ---
if not os.path.exists(INPUT_MP3):
    download_from_blob(INPUT_MP3)

if not os.path.exists(INPUT_JSON):
    download_from_blob(INPUT_JSON)

# --- 2. Läs metadata ---
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    metadata = json.load(f)

episode_title = metadata.get("title", f"Episode {datetime.date.today()}")
episode_description = metadata.get("description", "Automatiskt publicerat av workflow")
episode_date = metadata.get("date", str(datetime.date.today()))

# --- 3. Namnge filen ---
episode_file = f"episode_{episode_date}.mp3"
if INPUT_MP3 != episode_file:
    os.rename(INPUT_MP3, episode_file)

# --- 4. Skapa episod ---
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
