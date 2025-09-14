# Best Practice – Skriva till Azure Blob (AFP-projektet)

Den här guiden sammanfattar hur vi skriver och läser från Azure Blob i AFP-projektet. Följer vi detta mönster slipper vi specialfall och env-strul.

---

## 🔑 Grundprinciper

1. **All I/O via `azure_blob.py`**
   - Använd `azure_blob.upload_json`, `azure_blob.put_text`, `azure_blob.put_bytes`.
   - Läs via `azure_blob.get_json` eller `azure_blob.get_text`.
   - Kolla existens via `azure_blob.exists`.
   - Ingen lokal filhantering (`open()`, `Path`) i scripts.

2. **Environment-variabler**
   - Koden använder alltid:
     
        CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
     
   - Workflow mappar secrets till env (vi använder en container-SAS-URL):
     
        AZURE_STORAGE_CONTAINER: ${{ secrets.AZURE_CONTAINER }}
        BLOB_CONTAINER_SAS_URL:  ${{ secrets.BLOB_CONTAINER_SAS_URL }}

3. **Skrivning (JSON)**
   
        from src.storage import azure_blob
        data = {"apps": 30, "goals": 22}
        path = f"stats/players/{player_id}/{season}.json"
        azure_blob.upload_json(CONTAINER, path, data)

4. **Skrivning (text / bytes)**
   
        from src.storage import azure_blob
        azure_blob.put_text(CONTAINER, "logs/run.txt", "Job completed successfully")

5. **Iteration / listor**
   - Använd `list_prefix` endast när nödvändigt.
   - Bygg hellre loopar utifrån manifest eller players_history.
   - Om `list_prefix` används måste SAS ha `l`-rättighet.

6. **Error handling (tidig guard)**
   
        if not CONTAINER or not CONTAINER.strip():
            raise RuntimeError("AZURE_STORAGE_CONTAINER is missing or empty")

7. **Workflow-mönster (GitHub Actions)**
   
        env:
          AZURE_STORAGE_ACCOUNT:   ${{ secrets.AZURE_STORAGE_ACCOUNT }}
          AZURE_STORAGE_CONTAINER: ${{ secrets.AZURE_CONTAINER }}
          BLOB_CONTAINER_SAS_URL:  ${{ secrets.BLOB_CONTAINER_SAS_URL }}
          AZURE_CLIENT_ID:         ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_TENANT_ID:         ${{ secrets.AZURE_TENANT_ID }}
          AZURE_CLIENT_SECRET:     ${{ secrets.AZURE_CLIENT_SECRET }}

---

## 📌 Sammanfattning

- **Koden** läser alltid containern från `AZURE_STORAGE_CONTAINER` (fallback `afp`).
- **Workflow** mappar `AZURE_CONTAINER` → `AZURE_STORAGE_CONTAINER` och injicerar `BLOB_CONTAINER_SAS_URL`.
- **All läs/skriv** sker via `azure_blob.py`.
- **Guards + enkel echo** i workflow förenklar felsökning.
- **Curated källor** (manifest/history) föredras framför att lista blobar när det går.
