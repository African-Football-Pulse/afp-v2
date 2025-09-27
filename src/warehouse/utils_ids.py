import pandas as pd

def normalize_ids(df: pd.DataFrame, cols=None) -> pd.DataFrame:
    """
    Normalisera ID-kolumner till str utan '.0' och med tom sträng för NaN.
    cols: lista med kolumner att normalisera, default = ["player_id", "assist_id", "player_in_id", "player_out_id"]
    """
    if cols is None:
        cols = ["player_id", "assist_id", "player_in_id", "player_out_id"]

    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .str.replace(".0", "", regex=False)
                .str.strip()
            )
    return df
