import yaml
from datetime import datetime, timezone
from src.collectors import collect_match_details


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def run_all(with_api=True):
    """Loopar över alla aktiva ligor och hämtar matchdetaljer baserat på veckans manifest."""
    date_str = today_str()

    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    print(f"[collect_extract_weekly] Found {len(leagues)} leagues in config.")

    for league in leagues:
        if not league.get("active", False):
            continue
        league_id = league["id"]
        name = league.get("name", str(league_id))
        manifest_path = f"stats/weekly/{date_str}/{league_id}/manifest.json"
        print(f"[collect_extract_weekly] Using manifest for {name} ({league_id}): {manifest_path}")

        collect_match_details.run(
            league_id=league_id,
            manifest_path=manifest_path,
            with_api=with_api,
            mode="weekly"
        )
        print(f"[collect_extract_weekly] Done processing {name} ({league_id})")


if __name__ == "__main__":
    run_all()
