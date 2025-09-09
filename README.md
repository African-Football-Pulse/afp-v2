# AFP – Data Pipeline Overview

Det här dokumentet beskriver i stora drag hur våra jobbflöden är uppbyggda och vad status är i dagsläget (sept 2025).  
Syftet är att ge en snabb onboarding för nya kollegor (t.ex. Adam) som ska förstå hur Collect/Produce m.m. hänger ihop.

---

## Översikt av pipeline

Hela systemet är uppdelat i ett antal steg:

1. **Collect**  
   - Hämtar nyhetsflöden (RSS etc.) och skriver normaliserade `items.json` till Blob Storage.  
   - Körs som **Azure Container Apps Job** på schema (regelbundet).  
   - Output: `collector/curated/news/<källa>/<liga>/<datum>/items.json`.

2. **Produce**  
   - Läser insamlade `items.json` från Blob.  
   - Bygger sektioner (t.ex. `S.STATS.TOP_AFRICAN_PLAYERS`, `S.OPINION.EXPERT_COMMENT`).  
   - Skriver `manifest.json` för varje sektion till Blob.  
   - Körs som **Azure Container Apps Job** på schema (regelbundet).  
   - Output: `producer/sections/<datum>/<SECTION_CODE>/manifest.json`.

3. **Assemble**  
   - Tar sektionerna från Produce och sätter ihop dem till hela avsnitt.  
   - Output: kompletta avsnittsmanifest.  
   - **Status:** körbart lokalt, **inte** deployat i Azure ännu.

4. **Render**  
   - Tar avsnittsmanifest och producerar ljudfiler (inkl. intro/outro, jinglar, TTS-röster).  
   - **Status:** körbart lokalt, **inte** deployat i Azure ännu.

5. **Publish**  
   - Laddar upp färdiga ljudfiler till distributionskanaler (t.ex. Buzzsprout, Spotify).  
   - **Status:** körbart lokalt, **inte** deployat i Azure ännu.

---

## Nuvarande status

✅ **Collect** – i drift (Azure Job, schemalagt).  
✅ **Produce** – i drift (Azure Job, schemalagt).  
⚪ **Assemble** – finns lokalt, ej deploy.  
⚪ **Render** – finns lokalt, ej deploy.  
⚪ **Publish** – finns lokalt, ej deploy.

> Med andra ord: vi har en regelbunden pipeline från nyheter (Collect) till sektioner (Produce). Nästa steg är att industrialisera Assemble/Render/Publish.

---

## Blob Storage-struktur

- **Collector output:**  
