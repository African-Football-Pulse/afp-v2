import os
import io
import pandas as pd
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
SEASON = "2025-2026"

def load_players_flat(season: str = SEASON) -> pd.DataFrame:
    """Ladda players_flat från warehouse"""
    path = f"warehouse/base/players_flat/{season}/players_flat.parquet"
    players_bytes = azure_blob.get_bytes(CONTAINER, path)
    df = pd.read_parquet(io.BytesIO(players_bytes))
    df["id"] = df["id"].astype(str)
    return df

def build_mapping(season: str = SEASON) -> pd.DataFrame:
    """
    Bygger en mapping mellan SoccerData-ID (från events)
    och AFP-ID (från players_flat).
    Just nu använder vi namn för att matcha, men kan byggas ut
    med fler källor (soccerway, wikipedia etc).
    """
    df = load_players_flat(season)

    mapping = []

    for _, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        afp_id = row["id"]

        if name:  # för nu bara namn
            mapping.append({"player_name": name, "afp_id": afp_id})

        # TODO: framtid – lägg till stöd för andra identifierare
        # ex: soccerway-id, opta-id, fifa-id

    return pd.DataFrame(mapping)

def map_events_to_afp(df_events: pd.DataFrame, season: str = SEASON) -> pd.DataFrame:
    """
    Lägg till en kolumn 'afp_id' i df_events baserat på mapping.
    Returnerar events med afrikanska spelare filtrerade.
    """
    df_map = build_mapping(season)

    # Merge på namn (första version)
    df = df_events.merge(
        df_map,
        how="left",
        left_on="player_name",
        right_on="player_name"
    )

    return df
