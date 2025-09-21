import requests
from bs4 import BeautifulSoup
import json
import re
import os
from src.storage import azure_blob

EPL_CLUBS = {
    "Arsenal": "https://en.wikipedia.org/wiki/Arsenal_F.C.",
    "Aston Villa": "https://en.wikipedia.org/wiki/Aston_Villa_F.C.",
    "Bournemouth": "https://en.wikipedia.org/wiki/A.F.C._Bournemouth",
    "Brentford": "https://en.wikipedia.org/wiki/Brentford_F.C.",
    "Brighton": "https://en.wikipedia.org/wiki/Brighton_%26_Hove_Albion_F.C.",
    "Burnley": "https://en.wikipedia.org/wiki/Burnley_F.C.",
    "Chelsea": "https://en.wikipedia.org/wiki/Chelsea_F.C.",
    "Crystal Palace": "https://en.wikipedia.org/wiki/Crystal_Palace_F.C.",
    "Everton": "https://en.wikipedia.org/wiki/Everton_F.C.",
    "Fulham": "https://en.wikipedia.org/wiki/Fulham_F.C.",
    "Leeds United": "https://en.wikipedia.org/wiki/Leeds_United_F.C.",
    "Liverpool": "https://en.wikipedia.org/wiki/Liverpool_F.C.",
    "Manchester City": "https://en.wikipedia.org/wiki/Manchester_City_F.C.",
    "Manchester United": "https://en.wikipedia.org/wiki/Manchester_United_F.C.",
    "Newcastle United": "https://en.wikipedia.org/wiki/Newcastle_United_F.C.",
    "Nottingham Forest": "https://en.wikipedia.org/wiki/Nottingham_Forest_F.C.",
    "Sunderland": "https://en.wikipedia.org/wiki/Sunderland_A.F.C.",
    "Tottenham Hotspur": "https://en.wikipedia.org/wiki/Tottenham_Hotspur_F.C.",
    "West Ham United": "https://en.wikipedia.org/wiki/West_Ham_United_F.C.",
    "Wolverhampton Wanderers": "https://en.wikipedia.org/wiki/Wolverhampton_Wanderers_F.C.",
}

AZURE_PATH = "meta/2025-2026/epl_squads.json"
LOCAL_FALLBACK = "epl_squads.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def find_squad_tables(soup):
    """Hitta alla tabeller som innehåller spelartrupper."""
    # Vanlig struktur på Wikipedia: football-squad nogrid (två tabeller per klubb)
    tables = soup.find_all("table", {"class": re.compile("football-squad nogrid")})
    if not tables:
        # Fallback: gamla enklare variant
        tables = soup.find_all("table", {"class": "wikitable"})
    return tables


def scrape_club_squad(club, url):
    print(f"🔎 Scraping {club}...")
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    squads = []
    tables = find_squad_tables(soup)
    if not tables:
        print(f"⚠️ No valid squad tables found for {club}")
        return squads

    for table in tables:
        for row in table.find_all("tr")[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cols) < 4:
                continue
            # Ta bara de första 4 kolumnerna (No, Pos, Nation, Player)
            number, pos, nation, name = cols[:4]
            if not name:
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

    print(f"📊 Totalt {total_players} spelare scraped för alla klubbar")

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
