# Mall för statistik-sektioner i AFP

Detta dokument beskriver hur man lägger till och bygga nya STATS-sektioner i AFP-projektet.  
Alla statistik-sektioner följer samma struktur för att fungera med **stats-drivern** (`s_stats_driver.py`).

---

## 1. Översikt

Flöde för STATS-sektioner:

(flowchart i mermaid)

    collect (matcher) → matches.json i Azure  
    matches.json → stats_utils.extract_african_events  
    → african_events.json → s_stats_driver.py  
    → s_stats_top_performers_round / s_stats_discipline / s_stats_goal_impact  
    → sections/<SECTION>/<DATE>/<LEAGUE>/_/  
    → section.md, section.json, section_manifest.json

---

## 2. Centrala script

- src/producer/stats_utils.py  
  Helpers för att:
  - ladda masterlistan (players_africa_master.json)
  - extrahera events för afrikanska spelare
  - spara african_events.json
  - hantera state (last_stats.json)
  - lista datum-mappar i Azure
  - hitta nästa runda att köra

- src/sections/s_stats_driver.py  
  Driver som:
  - loopar alla ligor
  - hittar nya rundor via stats_utils.find_next_round
  - anropar varje STATS-sektion i STATS_SECTIONS-listan
  - sparar state efter körning

- src/sections/s_stats_<namn>.py  
  Själva statistik-sektionen, implementerar:
  def build_section(season, league_id, round_dates, output_prefix):
      ...
      return section_utils.write_outputs(...)

---

## 3. Steg för steg – skapa en ny sektion

1. Skapa filen i src/sections/, t.ex. s_stats_discipline.py.  
   Implementera build_section enligt mallen.

2. Lägg till sektionen i STATS_SECTIONS-listan i s_stats_driver.py.  

3. Lägg till sektionen i sections_library.yaml:  
   S.STATS.DISCIPLINE:  
     module: s_stats_discipline  
     runner: build_section  
     description: Discipline stats for African players  

4. Kör produce med JOB_TYPE=produce och kontrollera att ny sektion genereras i Azure:  
   sections/S.STATS.DISCIPLINE/<DATE>/<LEAGUE>/_/

---

## 4. Tips
- Alla sektioner ska använda helpers i stats_utils.  
- Output ska alltid vara section.json, section.md och section_manifest.json.  
- Om ingen data hittas → returnera None så produce_auto loggar snyggt.  
- Håll loggningen konsekvent: `[s_stats_<namn>] …`.

