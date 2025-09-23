import os
from src.producer import stats_utils
from src.sections import s_stats_top_performers_round

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def build_section(args=None):
    """
    Driver för alla STATS-sektioner.
    Loopar igenom ligor i stats/{season}/, hittar nya rundor och
    anropar de stats-sektioner som ska byggas.
    """
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    outdir = getattr(args, "outdir", "sections")

    results = []

    # Lista alla ligor i stats/{season}/
    prefix = f"stats/{season}/"
    blobs = stats_utils.azure_blob.list_prefix(CONTAINER, prefix)
    league_ids = sorted({b.split("/")[2] for b in blobs if len(b.split("/")) >= 3})

    # Ladda state
    state = stats_utils.load_last_stats()

    for league_id in league_ids:
        # Hitta nästa runda för denna liga
        round_dates = stats_utils.find_next_round(season, int(league_id))
        if not round_dates:
            continue

        # 👉 Här kallar vi våra stats-sektioner
        res = s_stats_top_performers_round.build_section(
            season=season,
            league_id=int(league_id),
            round_dates=round_dates,
            output_prefix=f"{outdir}/S.STATS.TOP_PERFORMERS_ROUND/{season}/{league_id}"
        )
        if res:
            results.append(res)
            # Uppdatera state för denna liga
            state[str(league_id)] = round_dates[-1]

    # Spara tillbaka state
    if results:
        stats_utils.save_last_stats(state)

    return results
