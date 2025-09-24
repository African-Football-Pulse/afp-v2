# src/warehouse/build_clean_sheets_africa.py

import json
import pandas as pd
from io import BytesIO
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def load_parquet_from_blob(container: str, path: str) -> pd.DataFrame:
    """Ladda parquet från Azure Blob till Pandas."""
    blob_bytes = (
        azure_blob._client()
        .get_container_client(container)
        .get_blob_client(path)
        .download_blob()
        .readall()
    )
    return pd.read_parquet(BytesIO(blob_bytes), engine="pyarrow")


def main():
    container = "afp"

    # 📥 Masterlista (id, namn, land)
    master_path = "players/africa/players_africa_master.json"
    master = load_json_from_blob(container, master_path)
    master_players = master.get("players", [])

    id_to_name = {str(p["id"]): p["name"] for p in master_players if str(p.get("id")).isdigit()}
    name_to_id = {p["name"]: str(p["id"]) for p in master_players if str(p.get("id")).isdigit()}
    id_to_country = {str(p["id"]): p["country"] for p in master_players if str(p.get("id")).isdigit()}

    # 📥 Players by country (pos info)
    by_country_path = "players/africa/players_by_country.json"
    players_by_country = load_json_from_blob(container, by_country_path)

    # Filtrera målvakter (GK)
    gks = []
    for code, players in players_by_country.items():
        for p in players:
            if p.get("pos") == "GK":
                gks.append(p)

    unmatched_players = []
    gk_map = {}
    for gk in gks:
        name = gk.get("name")
        if name in name_to_id:
            gk_map[name] = name_to_id[name]
        else:
            unmatched_players.append(name)

    # 📥 Player match stats
    stats_path = "warehouse/base/player_match_stats.parquet"
    df_stats = load_parquet_from_blob(container, stats_path)
    df_stats["player_id"] = df_stats["player_id"].astype(str)

    # 📥 Matches flat (för resultat)
    all_files = azure_blob.list_prefix(container, "warehouse/base/matches_flat/")
    parquet_files = [f for f in all_files if f.endswith(".parquet")]

    if not parquet_files:
        print("[build_clean_sheets_africa] ⚠️ Inga matches_flat-filer hittades")
        return

    df_matches_all = []
    for path in parquet_files:
        try:
            df_tmp = load_parquet_from_blob(container, path)
            df_matches_all.append(df_tmp)
        except Exception as e:
            print(f"[build_clean_sheets_africa] ⚠️ Kunde inte läsa {path}: {e}")
            continue
    if not df_matches_all:
        print("[build_clean_sheets_africa] ⚠️ Ingen matchdata laddad")
        return

    df_matches = pd.concat(df_matches_all, ignore_index=True)

    # 🔎 Koppla ihop målvakter med matcher
    rows = []
    unmatched_matches = []

    for _, row in df_stats.iterrows():
        pid = row["player_id"]
        season = row["season"]
        league_id = row["league_id"]
        match_id = row["match_id"]

        # Bara målvakter
        pname = id_to_name.get(pid)
        if pname not in gk_map:
            continue

        # Slå upp matchresultat
        match = df_matches[df_matches["match_id"] == match_id]
        if match.empty:
            unmatched_matches.append(match_id)
            continue

        m = match.iloc[0]
        home_goals = m.get("home_goals")
        away_goals = m.get("away_goals")
        home_team_id = m.get("home_team_id")
        away_team_id = m.get("away_team_id")

        # Här borde vi egentligen veta vilken klubb spelaren tillhörde i matchen
        # → tills vidare antar vi att clean sheet räknas om matchen hade 0 insläppta
        clean_sheet = 0
        if (home_goals == 0) or (away_goals == 0):
            clean_sheet = 1

        rows.append({
            "player_id": pid,
            "player_name": pname,
            "country": id_to_country.get(pid),
            "season": season,
            "clean_sheets": clean_sheet
        })

    if not rows:
        print("[build_clean_sheets_africa] ⚠️ Ingen clean sheet-data hittades")
        return

    df_result = pd.DataFrame(rows)
    grouped = df_result.groupby(["player_id", "season"]).agg(
        clean_sheets=("clean_sheets", "sum")
    ).reset_index()

    grouped["player_name"] = grouped["player_id"].map(id_to_name)
    grouped["country"] = grouped["player_id"].map(id_to_country)

    # 📦 Spara till Parquet
    parquet_bytes = grouped.to_parquet(index=False, engine="pyarrow")
    output_path = "warehouse/metrics/clean_sheets_africa.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    print(f"[build_clean_sheets_africa] ✅ Uploaded {len(grouped)} rows → {output_path}")

    # 👀 Preview
    print("\n[build_clean_sheets_africa] 🔎 Sample:")
    print(grouped.head(10).to_string(index=False))

    # 👀 Felhantering
    if unmatched_players:
        print("\n[build_clean_sheets_africa] ⚠️ Unmatched GK players (not in masterlist):")
        for name in unmatched_players:
            print(" -", name)
    if unmatched_matches:
        print(f"\n[build_clean_sheets_africa] ⚠️ {len(unmatched_matches)} matches could not be resolved to results")


if __name__ == "__main__":
    main()
