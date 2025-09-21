import requests
from bs4 import BeautifulSoup
import json
import re
import os
from src.storage import azure_blob

EPL_CLUBS = {
    "Arsenal": "https://en.wikipedia.org/wiki/Arsenal_F.C.",
    "Aston Villa": "https://en.wikipedia.org/wiki/Aston_Villa_F.C.",
}

AZURE_PATH = "meta/2025-2026/epl_squads_test.json"
LOCAL_FALLBACK = "epl_squads_test.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

def scrape_club_squad(club, url):
    print(f"🔎 Scraping {club}...")
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    squads = []

    # Leta efter rubrik "First-team squad"
    header = soup.find(["span", "h2", "h3"], string=re.compile("First-team squad", re.I))
    if not header:
        header = soup.find(id=re.compile("First-team_squad", re.I))
    if not header:
        print(f"⚠️ No First-team squad header found for {club}")
        return squads

    # Hitta nästa tabell efter rubriken
    table = header.find_next("table", {"class": "wikitable"})
    if not table:
        print(f"⚠️ No squad table found for {club}")
        return squads

    for row in table.find_all("tr")[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if len(cols) < 4:
            continue
        try:
            number = cols[0]
            pos = cols[1]
            nation = cols[2]
            name = cols[3]
        except Exception:
            continue

        squads.append({
            "no": number,
            "pos": pos,
            "nation": nation,
            "name": name
        })

    print(f"   ➡️ Found {len(squads)} players for {club}")
    return squads

def main():
    all_squads = {}
    total_players = 0

    for club, url in EPL_CLUBS.items():
        try:
            squad = scrape_club_squad(club, url)
            all_squads[club] = squad
            total_players += len(squad)
        except Exception as e:
            print(f"⚠️ Error scraping {club}: {e}")
            all_squads[club] = []

    print(f"📊 Totalt {total_players} spelare scraped (test: Arsenal + Aston Villa)")

    data = json.dumps(all_squads, indent=2, ensure_ascii=False)

    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")
    try:
        azure_blob.put_text(container, AZURE_PATH, data)
        print(f"✅ Saved EPL squads to Azure: {AZURE_PATH}")
    except Exception as e:
        print(f"⚠️ Azure upload failed: {e}")
        with open(LOCAL_FALLBACK, "w", encoding="utf-8") as f:
            f.write(data)
        print(f"💾 Saved EPL squads locally as fallback: {LOCAL_FALLBACK}")

if __name__ == "__main__":
    main()
