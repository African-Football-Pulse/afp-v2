# AFP Jobs – Struktur & Samband

Det här repot innehåller definitioner för tre Azure Container Apps Jobs som kör olika delar av **African Football Pulse**-pipeline.  
För att undvika förvirring används en konsekvent namngivning genom hela kedjan: **Azure-resurs → YAML-fil → JOB_TYPE → Python-modul**.

---

## Översikt

| Azure Job (resursnamn) | YAML-fil                          | JOB_TYPE (env) | Python-modul som körs                          |
|-------------------------|-----------------------------------|----------------|------------------------------------------------|
| `afp-collect-job`       | `.github/workflows/collect-job.yaml`   | `collect`      | `src.collectors.rss_multi`                     |
| `afp-produce-job`       | `.github/workflows/produce-job.yaml`   | `produce`      | `src.sections.s_news_top3_guardian`            |
| `afp-assemble-job`      | `.github/workflows/assemble-job.yaml`  | `assemble`     | `src.assembler.main`                           |

---

## Hur det fungerar

- **Entrypoint (`job_entrypoint.py`)**
  - Läser `JOB_TYPE` från env.
  - Matchar värdet mot `JOBS`-mappningen och kör motsvarande Python-modul.
  - Default är `collect` om `JOB_TYPE` saknas.

- **YAML-filer**
  - Beskriver varje jobb (`collect`, `produce`, `assemble`).
  - Sätter rätt `JOB_TYPE` och `BLOB_PREFIX`.
  - Refererar till hemligheten `BLOB_CONTAINER_SAS_URL` som injiceras från GitHub Secrets → Azure Secrets.

- **Deploy-workflow (`.github/workflows/deploy.yml`)**
  - Loggar in i Azure.
  - Synkar in hemligheten `BLOB_CONTAINER_SAS_URL` till alla tre jobben i Azure.
  - Uppdaterar respektive jobb med dess YAML-fil.

---

## Secrets

- `BLOB_CONTAINER_SAS_URL` – full SAS-URL för Azure Blob Container.
  - Ligger som **GitHub Secret** i repot.
  - Sätts in i varje Azure Job via `az containerapp job secret set`.
  - Används i koden för att läsa/skriva blobs.

---

## Flöde vid deployment

1. **Push till `main`** eller **manuell trigger** startar workflow `deploy.yml`.
2. `deploy.yml` loggar in i Azure och sätter/uppdaterar `BLOB_CONTAINER_SAS_URL` för alla tre jobs.
3. Varje job uppdateras med motsvarande YAML-definition (`collect-job.yaml`, `produce-job.yaml`, `assemble-job.yaml`).
4. När jobben körs i Azure används `job_entrypoint.py` för att starta rätt modul utifrån `JOB_TYPE`.

---

## Viktiga kommandon

- Uppdatera secrets i Azure (körs normalt via Actions):
  ```bash
  az containerapp job secret set --resource-group <RG> --name afp-collect-job --secrets BLOB_CONTAINER_SAS_URL="<SAS-URL>"
