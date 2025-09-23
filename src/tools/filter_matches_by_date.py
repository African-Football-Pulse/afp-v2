import argparse
import os
from src.storage import azure_blob

def filter_matches(league_id: int, season: str, date: str, container: str = None):
    container = container or os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    src_path = f"stats/{season}/{league_id}/matches.json"
    data = azure_blob.get_json(container, src_path)   # ✅ rätt metod

    if not data:
        print(f"[filter_matches_by_date] ❌ Kunde inte hämta {src_path} från Azure")
        return

    matches = []
    leagues = data if isinstance(data, list) else [data]

    for league in leagues:
        for stage in league.get("stage", []):
            for m in stage.get("matches", []):
                if m.get("date") == date:
                    matches.append(m)

    if not matches:
        print(f"[filter_matches_by_date] ⚠️ Hittade inga matcher för {date} i liga {league_id}")
        return

    safe_date = date.replace("/", "-")
    out_path = f"stats/{season}/{league_id}/{safe_date}/matches.json"
    azure_blob.upload_json(container, out_path, matches)
    print(f"[filter_matches_by_date] ✅ Uploaded {len(matches)} matches to {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("league_id", type=int)
    parser.add_argument("season", type=str)
    parser.add_argument("date", type=str, help="format: DD/MM/YYYY")
    parser.add_argument("--container", type=str, default=None)
    args = parser.parse_args()

    filter_matches(args.league_id, args.season, args.date, container=args.container)


if __name__ == "__main__":
    main()
