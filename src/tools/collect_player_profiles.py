import os
import json
import argparse
import requests
from bs4 import BeautifulSoup
from src.storage import azure_blob

SOCCERDATA_AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

USER_AGENT = "AfricanFootballPulseBot/1.0 (contact: patrik@africanfootballpulse.com)"


def fetch_soccerdata_player(player_id: int):
    """Fetch structured player data from SoccerData API"""
    url = "https://api.soccerdataapi.com/player/"
    params = {"player_id": player_id, "auth_token": SOCCERDATA_AUTH_KEY}
    headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_wikipedia_profile(wiki_url: str):
    """Fetch summary and infobox data from Wikipedia"""
    title = wiki_url.split("/wiki/")[-1]
    api_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|pageprops|categories",
        "titles": title,
        "explaintext": 1,
    }
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(api_url, params=params, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return {}

    page = next(iter(pages.values()))
    extract = page.get("extract", "")

    # Basic infobox parsing (optional improvement: parse HTML via BeautifulSoup)
    personal = {}
    if "born" in extract.lower():
        # very naive parsing, but can be expanded
        lines = extract.split("\n")
        for line in lines[:20]:  # scan first lines for personal info
            if "born" in line.lower():
                personal["date_of_birth"] = line.strip()

    return {"personal": personal, "summary": extract[:2000]}  # limit summary


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
            "summary": wiki.get("summary", f"{name} is a professional footballer."),
            "personal": wiki.get("personal", {}),
            "club_career": None,
            "international_career": None,
            "style_of_play": None,
            "career_statistics": {},
            "honours": [],
            "trivia": [],
        },
    }

    return profile


def save_profile(profile: dict):
    pid = profile["id"]
    path = f"players/profiles/{pid}.json"
    azure_blob.upload_json(CONTAINER, path, profile)
    print(f"[collect_player_profiles] Uploaded profile for {profile['name']} → {path}")


def run_single(player_id: str):
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
    save_profile(profile)


def run_all():
    master = load_masterfile()
    players = master.get("players", [])
    print(f"[collect_player_profiles] Building profiles for {len(players)} players...")

    for i, p in enumerate(players, start=1):
        try:
            profile = build_profile(p)
            save_profile(profile)
            print(f"[{i}/{len(players)}] {p['name']} done.")
        except Exception as e:
            print(f"[ERROR] Failed for {p.get('name', p.get('id'))}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player-id", help="Player ID (SoccerData ID or AFRxxx)")
    parser.add_argument("--all", action="store_true", help="Process all players in masterfile")
    args = parser.parse_args()

    if args.all:
        run_all()
    elif args.player_id:
        run_single(args.player_id)
    else:
        print("⚠️ You must specify either --player-id or --all")


if __name__ == "__main__":
    main()

