import os
import json
import requests
from bs4 import BeautifulSoup
from src.storage import azure_blob

SOCCERDATA_AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def fetch_soccerdata_player(player_id: int):
    """Hämta spelardata från SoccerData API"""
    url = "https://api.soccerdataapi.com/player/"
    params = {"player_id": player_id, "auth_token": SOCCERDATA_AUTH_KEY}
    resp = requests.get(url, params=params, headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    return resp.json()

def fetch_wikipedia_profile(wiki_url: str):
    """Hämta infobox + text från Wikipedia"""
    title = wiki_url.split("/wiki/")[-1]
    api_url = f"https://en.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": title,
        "format": "json",
        "prop": "text|sections"
    }
    resp = requests.get(api_url)
    resp.raise_for_status()
    data = resp.json()

    # extrahera HTML
    html = data["parse"]["text"]["*"]
    soup = BeautifulSoup(html, "html.parser")

    # Infobox
    infobox = soup.find("table", {"class": "infobox"})
    personal = {}
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

    # Career statistics table
    career_stats = []
    for table in soup.find_all("table", {"class": "wikitable"}):
        if "Apps" in table.text and "Goals" in table.text:
            for row in table.find_all("tr")[1:]:
                cols = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
                if len(cols) >= 3 and cols[0].isdigit() or "-" in cols[0]:
                    career_stats.append(cols)

    return {"personal": personal, "career_statistics_raw": career_stats}

def build_profile(player_id: int, name: str, wiki_url: str):
    """Bygg en spelarprofil med SoccerData + Wikipedia"""
    soccerdata = fetch_soccerdata_player(player_id)
    wiki = fetch_wikipedia_profile(wiki_url)

    profile = {
        "id": player_id,
        "name": name,
        "sources": {
            "wikipedia": wiki_url,
            "soccerdata": f"players/africa/{player_id}.json"
        },
        "profile": {
            "summary": soccerdata.get("name", name) + " is a professional footballer.",
            "personal": wiki.get("personal", {}),
            "club_career": None,  # TODO: hämta textavsnitt
            "international_career": None,  # TODO
            "style_of_play": None,  # TODO
            "career_statistics": {
                "by_club": wiki.get("career_statistics_raw", [])
            },
            "honours": [],
            "trivia": []
        }
    }
    return profile

def main():
    # Testa med Wissam Ben Yedder
    player_id = 56780
    name = "Wissam Ben Yedder"
    wiki_url = "https://en.wikipedia.org/wiki/Wissam_Ben_Yedder"

    profile = build_profile(player_id, name, wiki_url)

    # Spara till Azure
    path = f"players/profiles/{player_id}.json"
    azure_blob.upload_json(CONTAINER, path, profile)
    print(f"[collect_player_profiles] Uploaded profile for {name} → {path}")

if __name__ == "__main__":
    main()
