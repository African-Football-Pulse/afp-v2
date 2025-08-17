# AFP v2 (Modular, Section-First Architecture)

Sections are the atomic unit. Pipelines:
1) collect_data → raw → curated (+ input_manifest.json)
2) produce_sections → section.txt + (later TTS) section.mp3 + section_manifest.json
3) assemble_episode → episode_script.txt + episode_manifest.json (+ later mp3)

Storage: Azure Blob (not Git). GitHub Actions only ships logs/manifests.

## Quick start
1. Add GitHub Secrets:
   - AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID
   - AZURE_STORAGE_ACCOUNT (e.g., afpstoragepilot)
   - AZURE_CONTAINER (e.g., afp)
2. Run workflow `build_and_push` to build/push image to ACR.
3. In Azure Container Apps Jobs, use image `afpacr.azurecr.io/afp-runner:latest`:
   - collect:   python -m src.collectors.collect_data
   - produce:   python -m src.sections.s_top_african_players
   - assemble:  python -m src.assembler.build_episode

Artifacts will appear under:
raw/, curated/, sections/, episodes/ in your Blob container.
