# 📖 Naming Convention – Warehouse Parquet Files

## 🔑 Grundprincip
Namnen på Parquet-filer i **Warehouse** ska återspegla **nivån i datalagret** och vilken typ av transformation som gjorts.  
Detta ger konsekvens, gör det lättare att navigera, och underlättar felsökning.  

---

## 1. RAW → BASE (flattenings)
**När används:** När vi tar JSON eller nested data från RAW och gör om det till tabeller (rader × kolumner).  
**Suffix:** `_flat`  

**Exempel:**
- `warehouse/base/players_flat.parquet` → masterlista spelare (flattenad från `players_africa_master.json`)  
- `warehouse/base/teams_flat.parquet` → alla lag (från `teams/<league>/<team>.json`)  
- `warehouse/base/leagues_flat.parquet` → ligor  
- `warehouse/base/seasons_flat.parquet` → säsonger  
- `warehouse/base/matches_flat.parquet` → matcher (från match-json)  
- `warehouse/base/events_flat.parquet` → events (mål, kort, assists)  
- `warehouse/base/countries_flat.parquet` → länder  

👉 Dessa är **tekniska flattenings** – rådata fast i kolumnärt format.

---

## 2. BASE (derived stats / aggregat)
**När används:** När vi bygger sammanfattad statistik ovanpå flattenings.  
**Suffix:** `_stats`, `_totals` eller annat beskrivande.  

**Exempel:**
- `warehouse/base/player_match_stats.parquet` → per spelare och match (agg från `events_flat`).  
- `warehouse/base/player_totals.parquet` → totals per spelare (agg från `player_match_stats`).  

👉 Dessa är **businesslogik** – summeringar och beräkningar.

---

## 3. METRICS (analytiska lager)
**När används:** När vi bygger KPI:er eller avancerade metrics för analys/rapportering.  
**Suffix:** Inget `_flat`, använd istället domännamn.  

**Exempel:**
- `warehouse/metrics/goals_africa.parquet` → mål per spelare/land.  
- `warehouse/metrics/assists_africa.parquet` → assists.  
- `warehouse/metrics/clean_sheets_africa.parquet` → clean sheets.  
- `warehouse/metrics/form_index.parquet` → formvärde per spelare.  
- `warehouse/metrics/milestones.parquet` → särskilda thresholds (50 mål, 100 matcher etc).  

👉 Dessa är **insiktslager** – direkt redo att användas i sektioner/content.  

---

## 4. CONTENT (sektioner)
**När används:** När vi producerar sektioner i pipeline.  
**Suffix:** Använd **sektion-ID** (från `sections_library.yaml`).  

**Exempel:**
- `sections/S.STATS.GOALSCORERS/section.md`  
- `sections/S.STATS.ASSISTS/section.json`  
- `sections/S.STATS.CLEAN_SHEETS/section_manifest.json`  

👉 Dessa är inte Parquet, utan text/JSON/manifest för content-produktion.  

---

## ✅ Sammanfattning

| Lager        | Typ                         | Namnstandard          | Exempel Path                                |
|--------------|-----------------------------|-----------------------|---------------------------------------------|
| RAW → BASE   | Flattenings av JSON         | `_flat`               | `warehouse/base/players_flat.parquet`       |
| BASE         | Derived stats / aggregat    | `_stats`, `_totals`   | `warehouse/base/player_match_stats.parquet` |
| METRICS      | KPI:er / insikter           | domännamn             | `warehouse/metrics/goals_africa.parquet`    |
| CONTENT      | Poddsektioner (ej parquet)  | sektion-ID            | `sections/S.STATS.GOALSCORERS/section.md`   |
