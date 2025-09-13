import os
import json
import argparse
import requests
from bs4 import BeautifulSoup
from src.storage import azure_blob

SOCCERDATA_AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def fetch_soccerdata_player(player_id: int):
    url = "https://api.soccerdataapi.com/player/"
    params = {"player_id": player_id, "auth_token": SOCCERDATA_AUTH_KEY}
    resp = requests.get(url, params=params, headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    return resp.json()


def fetch_wikipedia_profile(wiki_url: str):
    title = wiki_url.split("/wiki/")[-1]
    api_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": title,
        "format": "json",
        "prop": "text|sections"
    }
    resp = requests.get(api_url, params=params)
    resp.raise_for_status()
    data = resp.json()

    html = data["parse"]["text"]["*"]
    soup = BeautifulSoup(html, "html.parser")

    # Infobox extraction
    personal = {}
    infobox = soup.find("table", {"class": "infobox"})
    if infobox:
        for row in infobox.find_all("tr"):
            header = row.find("th")
            cell = row.find("td")
            if header and cell:
                key = header.text.strip()
                val = cell.text.strip()
                if "Date of birth" in key:
                    personal["date_of_birth"] = val
                elif "Place of birth" in key:
                    personal["place_of_birth"] = val
                elif "Height" in key:
                    personal["height"] = val
                elif "Position" in key:
                    personal["position"] = val
                elif "Current team" in key:
                    personal["current_team"] = val

    return {"personal": personal}


def load_masterfile():
    """Load players_africa_master.json from Azure"""
    path = "players/africa/players_africa_master.json"
    return azure_blob.get_json(CONTAINER, path)


def build_profile(player_meta: dict):
    pid = player_meta["id"]
    name = player_meta["name"]
    wiki_url = player_meta["sources"].get("wikipedia")

    soccerdata = None
    if isinstance(pid, int):  # Only fetch from SoccerData if we have a numeric ID
        try:
            soccerdata = fetch_soccerdata_player(pid)
        except Exception as e:
            print(f"[WARN] SoccerData fetch failed for {name}: {e}")

    wiki = {}
    if wiki_url:
        try:
            wiki = fetch_wikipedia_profile(wiki_url)
        except Exception as e:
            print(f"[WARN] Wikipedia fetch failed for {name}: {e}")

    profile = {
        "id": pid,
        "name": name,
        "sources": player_meta.get("sources", {}),
        "profile": {
            "summary": f"{name} is a professional footballer.",
            "personal": wiki.get("personal", {}),
            "club_career": None,
            "international_career": None,
            "style_of_play": None,
            "career_statistics": {},
            "honours": [],
            "trivia": []
        }
    }

    return profile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player-id", required=True, help="Player ID (SoccerData ID or AFRxxx)")
    args = parser.parse_args()
    player_id = args.player_id

    print(f"[collect_player_profiles] Looking up {player_id} in masterfile...")

    master = load_masterfile()
    players = master.get("players", [])

    # Find player in masterfile
    player_meta = None
    for p in players:
        if str(p["id"]) == str(player_id):
            player_meta = p
            break

    if not player_meta:
        raise ValueError(f"Player {player_id} not found in masterfile!")

    profile = build_profile(player_meta)

    # Save to Azure
    path = f"players/profiles/{player_id}.json"
    azure_blob.upload_json(CONTAINER, path, profile)
    print(f"[collect_player_profiles] Uploaded profile for {profile['name']} → {path}")


if __name__ == "__main__":
    main()
