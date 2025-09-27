# 📚 Utils-moduler i AFP-projektet

Denna översikt sammanfattar alla identifierade `utils`-filer i projektet.  
Syftet är att tydliggöra ansvar, nyckelfunktioner och användningsområden per fil.

---

## 1. `src/sections/utils.py`

**Ansvar:**  
Gemensamma verktygsfunktioner för sektioner. Hjälper till att hantera in- och utdatafiler för varje sektion.

**Nyckelfunktioner:**
- `write_outputs(section_id, output_dir, md_text, json_data, manifest, status)`  
  Skriver ut **Markdown**, **JSON** och **manifest** för en sektion till Blob.  
- `load_json(path)` – laddar JSON från fil.  
- `save_json(path, data)` – sparar JSON till fil.  

**Användning:**  
Alla sektioner använder denna för konsekvent skrivning av output.

---

## 2. `src/producer/stats_utils.py`

**Ansvar:**  
Specialiserade funktioner för statistikbearbetning. Hjälper sektioner att summera, ranka och formattera statistik.

**Nyckelfunktioner:**
- `top_contributors(df, limit=5)` – returnerar de bästa spelarna baserat på mål + assist.  
- `discipline_table(df, limit=5)` – returnerar ranking baserat på gula och röda kort.  
- `goal_impact(df)` – beräknar ”impact score” för mål (t.ex. matchavgörande mål).  

**Användning:**  
Anropas av statistik-sektionerna för att generera tabeller och rankinglistor.

---

## 3. `src/producer/role_utils.py`

**Ansvar:**  
Hanterar roller/personas som används vid GPT-generering av text i sektionerna.

**Nyckelfunktioner:**
- `resolve_role(role_name, role_map)` – mappning från rollnamn till persona-ID (t.ex. `"storyteller" → "ak"`).  
- `apply_roles_to_prompt(prompt, role_map)` – ser till att GPT får rätt persona i prompten.  

**Användning:**  
Alla sektioner som använder GPT för textgenerering.

---

## 4. `src/producer/news_utils.py`

**Ansvar:**  
Hanterar nyhetskällor och nyhetsflöden i projektet.

**Nyckelfunktioner:**
- `load_news_items(path)` – laddar nyhetsartiklar (curated eller scored) från JSONL.  
- `filter_news_items(df, filters)` – filtrerar nyhetsartiklar baserat på t.ex. liga, spelare eller klubb.  
- `summarize_news(df)` – hjälper till att ta fram korta sammanfattningar.  

**Användning:**  
Nyhetssektioner (t.ex. `S.NEWS.TOP3.GENERIC`).

---

## 5. `src/warehouse/utils_ids.py`

**Ansvar:**  
Normalisera och hantera ID-kolumner i DataFrames för warehouse-data.

**Nyckelfunktioner:**
- `normalize_ids(df, cols=None)`  
  - Standardiserar ID-kolumner (`player_id`, `assist_id`, etc.).  
  - Konverterar till sträng, tar bort `.0`, fyller NaN med tom sträng.  

**Användning:**  
Alla warehouse-skript som läser events/matcher och behöver stabila ID-fält.

---

## 6. `src/warehouse/utils_mapping.py`

**Ansvar:**  
Koppla live events-data till AFP:s interna spelardata (`players_flat`).

**Nyckelfunktioner:**
- `load_players_flat(season)` – laddar `players_flat.parquet`.  
- `build_mapping(season)` – skapar en mapping-tabell mellan `player_name` och `afp_id`.  
- `map_events_to_afp(df_events, season)` – matchar events till AFP-spelare via namn och lägger till `afp_id`.  

**Användning:**  
Byggs in i alla scripts som behöver matcha live-data (events) mot `players_flat`.

---

# 📌 Slutsatser & rekommendationer

- **sections/utils.py** → alltid användas vid sektion-output.  
- **stats_utils.py** → används av alla statssektioner.  
- **role_utils.py** → säkerställer rätt persona i GPT.  
- **news_utils.py** → hanterar inläsning och filtrering av nyhetskällor.  
- **utils_ids.py** → standard för att normalisera ID:n i warehouse.  
- **utils_mapping.py** → central för att knyta live events till AFP-spelare.  

Alla scripts bör konsekvent importera rätt utils beroende på om de körs i `sections`, `producer` eller `warehouse`.
