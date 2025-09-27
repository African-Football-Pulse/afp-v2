import os
import io
import json
import pandas as pd
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
SEASON = "2025-2026"  # kan göras dynamiskt via argparse om du vill

def load_json_from_blob(container: str, path: str):
    """Ladda en JSON-fil från Azure Blob och returnera som dict."""
    text = azure_blob.get_text(container, path)
    return json.loads(text)

def build():
    # 🔹 1. Läs masterlistan över spelare (afrikanska)
    master_path = "players/africa/players_africa_master.json"
    print(f"[build_players_flat] ▶️ Laddar master från {master_path}")
    master = load_json_from_blob(CONTAINER, master_path)

    # 🔹 2. Gör om till DataFrame
    df_players = pd.DataFrame(master)

    # 🔹 TEMP FIX: säkerställ att player_id alltid är sträng
    if "player_id" in df_players.columns:
        df_players["player_id"] = df_players["player_id"].astype(str)

    # 🔹 3. Sätt utdata-path
    output_path = f"warehouse/base/players_flat/{SEASON}/players_flat.parquet"
    print(f"[build_players_flat] 💾 Skriver till {output_path}")

    # 🔹 4. Konvertera till bytes och skriv till Azure Blob
    buffer = io.BytesIO()
    df_players.to_parquet(buffer, index=False)
    buffer.seek(0)

    azure_blob.put_bytes(
        CONTAINER,
        output_path,
        buffer.getvalue(),
        content_type="application/octet-stream"
    )

    print(f"[build_players_flat] ✅ Klar, {len(df_players)} spelare sparade → {output_path}")

def main():
    build()

if __name__ == "__main__":
    main()
