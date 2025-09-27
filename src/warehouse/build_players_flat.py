import os
import json
import pandas as pd
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
SEASON = "2025-2026"

def load_json_from_blob(container: str, path: str):
    """Läs JSON från blob som text och returnera som dict"""
    text = azure_blob.get_text(container, path)
    return json.loads(text)

def main():
    master_path = "players/africa/players_africa_master.json"
    print(f"[build_players_flat] ▶️ Laddar master från {master_path}")
    master = load_json_from_blob(CONTAINER, master_path)

    # Normalisera JSON till DataFrame
    df = pd.json_normalize(master["players"])

    # Konvertera ID till sträng för att undvika ArrowInvalid
    df["id"] = df["id"].astype(str)

    # Logga för kontroll
    print("[build_players_flat] 🔍 DataFrame info:")
    print(df.dtypes)
    print(df.head(5).to_dict())

    # Output path
    output_path = f"warehouse/base/players_flat/{SEASON}/players_flat.parquet"
    print(f"[build_players_flat] 💾 Skriver till {output_path}")

    # Lagra till Parquet i blob
    import io
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    azure_blob.put_bytes(CONTAINER, output_path, buffer.getvalue(), content_type="application/octet-stream")

    print("[build_players_flat] ✅ Klar")

if __name__ == "__main__":
    main()
