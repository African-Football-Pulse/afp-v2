import yaml
from datetime import datetime, timezone
from src.collectors import collect_stats


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def run_all():
    """Loopar över alla aktiva ligor och hämtar veckans manifest."""
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    print(f"[collect_stats_weekly] Found {len(leagues)} leagues in config.")

    for league in leagues:
        if not league.get("active", False):
            continue
        league_id = league["id"]
        name = league.get("name", str(league_id))
        print(f"[collect_stats_weekly] Fetching weekly manifest for {name} ({league_id})")
        collect_stats.run(league_id=league_id, mode="weekly")


if __name__ == "__main__":
    run_all()
