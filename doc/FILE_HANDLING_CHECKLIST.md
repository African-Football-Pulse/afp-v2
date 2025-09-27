# AFP – Checklista för filhantering

Denna checklista säkerställer att vi alltid hanterar filer på rätt sätt i AFP-projektet.  
Syftet är att undvika lokala specialfall och säkerställa att all data hanteras konsekvent via **Azure Blob Storage**, med tydlig separation mellan **statiska resurser i imagen** och **dynamiska resurser i blobstorage**.

---

## 📂 1. Statiska resurser (följer med imagen)

Dessa filer **COPY:as in i Dockerfilen** och är en del av containerns "read-only" kodmiljö.  
Det är tillåtet att läsa dessa direkt via `open()` eller liknande, eftersom de är oföränderliga under runtime.

- **Källkod**  
  `/app/src/`

- **Konfiguration**  
  `/app/config/`

- **Jinglar & ljudassets**  
  `/app/assets/audio/`

- **Templates**  
  `/app/templates/`

✅ **Tillåtet**: Läsa dessa filer vid körning.  
❌ **Inte tillåtet**: Skriva/ändra dessa filer vid körning.

---

## ☁️ 2. Dynamiska resurser (hanteras via Azure Blob)

All data som uppstår vid körning (input, output, state, logs) ska **alltid** läsas/skrivas via `src/storage/azure_blob.py`.  
Detta gäller **alltid** – även för temporära resultat.

### Godkända metoder
- `azure_blob.upload_json(container, path, obj)`
- `azure_blob.put_text(container, path, text)`
- `azure_blob.put_bytes(container, path, data)`
- `azure_blob.get_json(container, path)`
- `azure_blob.get_text(container, path)`
- `azure_blob.exists(container, path)`
- `azure_blob.list_prefix(container, prefix)`

### Typiska kataloger i Blob
- **Stats**: `stats/{season}/{league_id}/...`
- **Sections**: `sections/<SECTION>/<DATE>/<LEAGUE>/_/`
- **State**: `sections/state/last_stats.json`, `sections/state/last_club.json`
- **Logs**: `logs/...`

✅ **Tillåtet**: Läsa/spara allt dynamiskt innehåll här.  
❌ **Inte tillåtet**: `open()` för att läsa/spara JSON, text, CSV, Parquet lokalt.

---

## 🔑 3. Miljövariabler & secrets

All åtkomst till Blob sker via standardiserade miljövariabler.  
Dessa sätts i **GitHub Actions** workflows (eller i secrets vid lokal testning med Docker).

- `AZURE_STORAGE_CONTAINER` → namnet på containern (default `afp`).
- `BLOB_CONTAINER_SAS_URL` → full SAS-URL för containern.  
- `AZURE_STORAGE_ACCOUNT`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` → för fallback med konto/identitet.

### Guard
Alla scripts bör ha en tidig kontroll:

```python
if not os.getenv("AZURE_STORAGE_CONTAINER"):
    raise RuntimeError("AZURE_STORAGE_CONTAINER is missing")
