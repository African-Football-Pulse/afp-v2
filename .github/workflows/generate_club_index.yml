name: Generate Club Index

on:
  workflow_dispatch:
  push:
    paths:
      - "players/africa/players_africa_master.json"
  schedule:
    - cron: "0 5 * * *"

jobs:
  generate:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run generate_club_index
        env:
          AZURE_STORAGE_ACCOUNT:   ${{ secrets.AZURE_STORAGE_ACCOUNT }}
          AZURE_STORAGE_CONTAINER: ${{ secrets.AZURE_CONTAINER }}
          BLOB_CONTAINER_SAS_URL:  ${{ secrets.BLOB_CONTAINER_SAS_URL }}
        run: |
          echo "🔄 Generating club index..."
          python -m src.tools.generate_club_index | tee generate_log.txt
          echo "✅ Done. See summary below:"
          echo "---- SUMMARY (last 20 lines) ----"
          tail -n 20 generate_log.txt
