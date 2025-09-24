import os
import requests
import datetime
import json

API_KEY = os.environ["BUZZSPROUT_API_KEY"]
PODCAST_ID = os.environ["BUZZSPROUT_PODCAST_ID"]

# Standardfil som laddats ner från blob
INPUT_FILE = "final_episode.mp3"

# Skapa filnamn baserat på datum (eller annan metadata senare)
today = datetime.date.today().strftime("%Y-%m-%d")
episode_file = f"episode_{today}.mp3"

# Döp om lokalt
os.rename(INPUT_FILE, episode_file)

# Titel och beskrivning (kan senare hämtas från era JSON-filer)
episode_title = f"Episode {today}"
episode_description = "Automatiskt publicerat av workflow"

# 1. Skapa episod
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
print(f"Created episode {episode_id}")

# 2. Ladda upp ljudfilen
with open(episode_file, "rb") as f:
    resp = requests.patch(
        f"https://api.buzzsprout.com/api/podcasts/{PODCAST_ID}/episodes/{episode_id}.json",
        headers={"Authorization": f"Token token={API_KEY}"},
        files={"audio_file": f}
    )
resp.raise_for_status()
print(f"Uploaded audio file for episode {episode_id}")
