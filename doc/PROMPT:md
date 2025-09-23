# AFP – Grundinstruktioner för ChatGPT

Detta dokument innehåller riktlinjer för hur ChatGPT ska användas i AFP-projektet.  
Syftet är att undvika missförstånd och säkerställa att alla flöden byggs på ett enhetligt sätt.

---

## 1. Arbetsprinciper
- All kod och alla data skrivs och läses från **Azure Blob Storage**.
- Vi arbetar **100 % i GitHub Web UI**. Ingen lokal kodhantering, inga lokala filer sparas.
- Vi testkör lokalt i Docker endast för att verifiera containers innan vi kör fulla flöden via GitHub Actions.
- Alla outputs (`.json`, `.md`, `manifest`) sparas i Azure via `src/storage/azure_blob.py`.

---

## 2. Jobb och entrypoints
- Flöden körs via `job_entrypoint` och styrs av miljövariabeln **JOB_TYPE**.
- **Collect-jobben** hämtar rådata (RSS, matcher, stats).
- **Produce-jobben** skapar sektioner (NEWS, OPINION, GENERIC, STATS) och laddar upp till Azure.

---

## 3. Sektioner
- Alla sektioner definieras i **sections_library.yaml** (vilken modul som körs).
- **produce_plan.yaml** definierar vilka sektioner som körs dagligen och med vilka args.
- Varje sektion skriver alltid tre filer i Azure:  
  - `section.md` (textinnehåll)  
  - `section.json` (JSON med text)  
  - `section_manifest.json` (metadata/manifest)  
- Helpers i `src/sections/utils.py` används för standardiserad output (`write_outputs`, `load_news_items` etc.).

---

## 4. NEWS / OPINION / GENERIC
- NEWS-sektioner bygger på `candidates.jsonl` och `scored.jsonl`.
- OPINION och GENERIC använder samma struktur men kan kombinera news och templates.
- **Club Highlight** har egen state-fil i Azure (`sections/state/last_club.json`) för att undvika upprepningar.

---

## 5. STATS (nytt upplägg)
- Alla STATS bygger på matcher i `stats/{season}/{league_id}/{date}/matches.json`.
- Vi skapar `african_events.json` genom att matcha masterlistan (`players/africa/players_africa_master.json`).
- State per liga lagras i `sections/state/last_stats.json` för att undvika dubbelkörningar.
- Vi använder en **driver** (`s_stats_driver.py`) som loopar över alla ligor och datum i Azure och anropar varje stats-sektion.
- Varje stats-sektion (t.ex. `s_stats_top_performers_round`, `s_stats_discipline`) har samma interface:  
  ```python
  build_section(season, league_id, round_dates, output_prefix)
