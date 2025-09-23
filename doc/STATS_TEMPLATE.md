# Mall för statistik-sektioner i AFP

Detta dokument beskriver hur man lägger till och bygga nya STATS-sektioner i AFP-projektet.  
Alla statistik-sektioner följer samma struktur för att fungera med **stats-drivern** (`s_stats_driver.py`).

---

## 1. Översikt

Flöde för STATS-sektioner:

```mermaid
flowchart TD
    A[collect (matcher)] --> B[matches.json i Azure]
    B --> C[stats_utils.extract_african_events]
    C --> D[african_events.json]
    D --> E[s_stats_driver.py]
    E --> F1[s_stats_top_performers_round]
    E --> F2[s_stats_discipline]
    E --> F3[s_stats_goal_impact]
    F1 & F2 & F3 --> G[sections/<SECTION>/<DATE>/<LEAGUE>/_/]
    G --> H1[section.md]
    G --> H2[section.json]
    G --> H3[section_manifest.json]
2. Centrala script
src/producer/stats_utils.py
Helpers för att:

ladda masterlistan (players_africa_master.json)

extrahera events för afrikanska spelare

spara african_events.json

hantera state (last_stats.json)

lista datum-mappar i Azure

hitta nästa runda att köra

src/sections/s_stats_driver.py
Driver som:

loopar alla ligor

hittar nya rundor via stats_utils.find_next_round

anropar varje STATS-sektion i STATS_SECTIONS-listan

sparar state efter körning

src/sections/s_stats_<namn>.py
Själva statistik-sektionen, implementerar:

python
Kopiera kod
def build_section(season: str, league_id: int, round_dates: list, output_prefix: str):
    ...
    return section_utils.write_outputs(...)
3. Steg för steg – skapa en ny sektion
Steg 1: Skapa sektionen
Skapa en ny fil i src/sections/, t.ex. s_stats_discipline.py.

Följ mallen:

python
Kopiera kod
import os
from collections import defaultdict
from src.storage import azure_blob
from src.producer import stats_utils
from src.sections import utils as section_utils

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def build_section(season: str, league_id: int, round_dates: list, output_prefix: str):
    blob_path = stats_utils.save_african_events(
        season=season, league_id=league_id, round_dates=round_dates, scope="round"
    )
    if not blob_path:
        return None

    events = azure_blob.get_json(CONTAINER, blob_path)
    if not events:
        return None

    # 🟢 Exempel: disciplin-statistik
    cards = defaultdict(lambda: {"yellow": 0, "red": 0, "name": ""})
    for ev in events:
        pid = ev["player"]["id"]
        pname = ev["player"]["name"]
        cards[pid]["name"] = pname
        if ev["event_type"] == "yellow_card":
            cards[pid]["yellow"] += 1
        elif ev["event_type"] == "red_card":
            cards[pid]["red"] += 1

    if not cards:
        return None

    # Sortera spelare med flest kort
    top_cards = sorted(cards.values(), key=lambda x: (x["red"], x["yellow"]), reverse=True)[:3]

    lines = [
        f"- {p['name']} ({p['yellow']} gult, {p['red']} rött)"
        for p in top_cards
    ]
    section_text = "Disciplinstatistik för omgången:\n" + "\n".join(lines)

    manifest = {
        "season": season,
        "league_id": league_id,
        "round_dates": round_dates,
        "count": len(top_cards),
        "players": [p["name"] for p in top_cards],
    }

    return section_utils.write_outputs(
        container=CONTAINER,
        prefix=output_prefix,
        section_id="S.STATS.DISCIPLINE",
        text=section_text,
        manifest=manifest,
    )
Steg 2: Lägg till i drivern
Öppna src/sections/s_stats_driver.py.

Lägg till modulen i STATS_SECTIONS:

python
Kopiera kod
from src.sections import (
    s_stats_top_performers_round,
    s_stats_discipline,   # 👈 ny sektion
)

STATS_SECTIONS = [
    {"id": "S.STATS.TOP_PERFORMERS_ROUND", "module": s_stats_top_performers_round},
    {"id": "S.STATS.DISCIPLINE", "module": s_stats_discipline},   # 👈 ny
]
Steg 3: Uppdatera sections_library.yaml
Lägg till en post för sektionen (för dokumentation och spårbarhet):

yaml
Kopiera kod
  S.STATS.DISCIPLINE:
    module: s_stats_discipline
    runner: build_section
    description: Yellow/red cards for African players
OBS: Drivern kallar modulen, men vi håller ändå kvar posterna i sections_library.yaml för översikt.

Steg 4: Kör produce_auto
produce_plan.yaml har bara en task för drivern:

yaml
Kopiera kod
  - section: S.STATS.DRIVER
    args:
      - --outdir
      - sections
      - --write-latest
Drivern loopar själv ligor och anropar alla definierade stats-sektioner.

4. Principer för alla STATS-sektioner
Interface är alltid:

python
Kopiera kod
build_section(season, league_id, round_dates, output_prefix)
Sektionen ska returnera section_utils.write_outputs(...).

All data hämtas via stats_utils.save_african_events för att återanvända logiken.

State (last_stats.json) hanteras av drivern, inte sektionen.

Alla outputs sparas i Azure på standardformatet:

php-template
Kopiera kod
sections/<SECTION>/<DATE>/<LEAGUE>/_/
5. Exempel på STATS-sektioner
Top Performers Round: mål, assist, kort → topp 3 spelare.

Discipline: flest gula/röda kort.

Goal Impact: spelare som gjort mål/assist som avgjorde matchen.

Minutes Played: flest spelminuter.

yaml
Kopiera kod

---
