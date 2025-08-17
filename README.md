# AFP v2 (Modular, Section-First Architecture)

**Goal:** Sections are the atomic unit. Pipelines:
1) `collect_data` → raw → curated (+ `input_manifest.json`)
2) `produce_sections` → `section.txt` + TTS `section.mp3` + `section_manifest.json`
3) `assemble_episode` → stitch sections → `episode.mp3` + manifest

**Storage:** Azure Blob (not Git). GitHub Actions only ships logs/manifests.

## Quick start
1. Add GitHub Secrets (Settings → Secrets and variables → Actions):
   - `AZURE_STORAGE_ACCOUNT`
   - `AZURE_STORAGE_KEY` **or** `AZURE_BLOB_SAS`
   - `AZURE_CONTAINER` (e.g. `afp`)
   - (later) `AZURE_SPEECH_KEY`, `AZURE_SPEECH_REGION`

2. Trigger **Collect** workflow (manually or wait for cron). It smoke-tests by writing:
   - `raw/hello.json`
   - `curated/demo/input_manifest.json`

3. Trigger **Produce Sections** → builds `S.TOP_AFRICAN_PLAYERS` (stub) to:
   - `sections/{YYYY-MM-DD}/premier_league/_/S.STATS.TOP_AFRICAN_PLAYERS/en/...`

4. Trigger **Assemble Episode** → creates a micro episode if min sections exist.

**Conventions:** All large artefacts live in Azure Blob:
```
raw/        curated/        sections/        episodes/
```
