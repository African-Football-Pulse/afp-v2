import pandas as pd
from src.storage import azure_blob

CONTAINER = "afp"


def main():
    input_path = "players/africa/africa_fifa_codes.json"
    output_path = "warehouse/base/countries_flat.parquet"

    print(f"[build_countries_flat] 🔄 Laddar {input_path}")
    data = azure_blob.get_json(CONTAINER, input_path)

    if not data:
        print("[build_countries_flat] ⚠️ Ingen data hittades i africa_fifa_codes.json")
        return

    # Data kan vara en dict {FIFA_code: country_name} eller en lista med dicts
    if isinstance(data, dict):
        rows = [{"FIFA_code": k, "country_name": v} for k, v in data.items()]
        df = pd.DataFrame(rows)
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        raise ValueError(f"[build_countries_flat] ❌ Ovänskat format på JSON: {type(data)}")

    if df.empty:
        print("[build_countries_flat] ⚠️ Tom dataframe, inget att spara")
        return

    # Säkerställ kolumnordning
    expected_cols = ["FIFA_code", "country_name"]
    df = df[[c for c in expected_cols if c in df.columns]]

    # 📦 Spara till Parquet
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(CONTAINER, output_path, parquet_bytes)

    print(f"[build_countries_flat] ✅ Uploaded {len(df)} rows → {output_path}")

    # 👀 Preview
    print("\n[build_countries_flat] 🔎 Sample:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
