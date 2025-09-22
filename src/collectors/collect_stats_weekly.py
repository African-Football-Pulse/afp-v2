import yaml
from datetime import datetime, timezone
from src.collectors import collect_stats

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def run_all():
    """Loopar över alla ligor som har enabled=true i leagues.yaml och kör collect_stats i weekly-läge."""
    date_str = today_str()

    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    print(f"[collect_stats_weekly] Found {len(leagues)} leagues in config.")

    for league in leagues:
        if not league.get("enabled", False):  # <-- fix: vi kollar enabled istället för active
            continue

        league_id = league["id"]
        name = league.get("name", str(league_id))

        print(f"[collect_stats_weekly] Processing {name} ({league_id}) for {date_str}...")
        try:
            collect_stats.run(
                league_id=league_id,
                mode="weekly",
                date=date_str
            )
            print(f"[collect_stats_weekly] ✅ Uploaded manifest for {name} ({league_id})")
        except Exception as e:
            print(f"[collect_stats_weekly] ❌ Failed for {name} ({league_id}): {e}")

if __name__ == "__main__":
    run_all()
