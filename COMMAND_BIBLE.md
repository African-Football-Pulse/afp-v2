## 🚀 Local Build & Run Guide (Produce Jobs)

### 1. Build image (från repo-root)
docker build -t afp-produce .

### 2. Run full produce plan (alla sektioner i config/produce_plan.yaml)
docker run --rm \
  -e JOB_TYPE=produce \
  -v $(pwd)/secrets/secret.json:/app/secrets/secret.json \
  afp-produce

### 3. Run a single section only (exempel: Daily Intro)

Kör på en rad:
docker run --rm -e JOB_TYPE=produce -v $(pwd)/secrets/secret.json:/app/secrets/secret.json afp-produce

docker run --rm \
  -e JOB_TYPE=produce \
  -v $(pwd)/secrets/secret.json:/app/secrets/secret.json \
  afp-produce \
  python -m src.produce_auto --plan config/produce_plan.yaml --only S.GENERIC.INTRO_DAILY

### 4. Verify output in Azure Blob
Alla producerade sektioner skrivs till:
sections/<SECTION_CODE>/<DATE>/<LEAGUE>/<TOPIC>/
med tre filer:
- section.json
- section.md
- section_manifest.json

### 🔑 Notes
- `secret.json` måste mountas in för att ladda OpenAI-nyckel + Blob SAS.
- `produce_auto` kör per default med UTC-datum (globalt konsekvent).
- Använd `--only` när du vill testa en sektion i taget.
