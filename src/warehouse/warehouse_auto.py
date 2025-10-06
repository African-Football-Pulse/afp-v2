import argparse
import subprocess
import yaml
from datetime import datetime, timedelta
import os
from glob import glob
from src.storage import azure_blob


# ------------------------------------------------------
# 🔧 Helpers
# ------------------------------------------------------

def run(cmd, env=None):
    """Helper för att köra subprocess och logga"""
    print(f"[warehouse_auto] ▶️ Kör: {' '.join(cmd)}")
    res = subprocess.run(["python", "-m"] + cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"Fel vid körning: {' '.join(cmd)}")
    else:
        print(res.stdout)


def ensure_fresh_matches(container: str, season: str, league: str, max_age_hours: int = 24, env=None):
    """
    Kontrollera att matches.json finns och är färsk, annars trigga collect_stats_fullseason.
    """
    match_path = f"stats/{season}/{league}/matches.json"
    print(f"[warehouse_auto] 🔍 Kontrollerar tillgång till {match_path}")

    # 1️⃣ Finns filen?
    if not azure_blob.exists(container, match_path):
        print(f"[warehouse_auto] ⚠️ Saknar {match_path} → kör collect_stats_fullseason")
        subprocess.run(["python", "-m", "src.collectors.collect_stats_fullseason"], check=True, env=env)
        return

    # 2️⃣ Kontrollera hur gammal filen är via blob-listning
    try:
        blobs = azure_blob.list_prefix(container, f"stats/{season}/{league}/matches.json")
        if not blobs:
            print(f"[warehouse_auto] ⚠️ Kunde inte läsa metadata → kör collect_stats_fullseason")
            subprocess.run(["python", "-m", "src.collectors.collect_stats_fullseason"], check=True, env=env)
            return

        # Eftersom list_prefix bara ger namn, kör vi last-modified via fetch i get_blob_client
        blob_client = azure_blob._client().get_container_client(container).get_blob_client(match_path)
        props = blob_client.get_blob_properties()
        last_modified = props["last_modified"].replace(tzinfo=None)
        age_hours = (datetime.utcnow() - last_modified).total_seconds() / 3600

        if age_hours > max_age_hours:
            print(f"[warehouse_auto] ⚠️ {match_path} är {age_hours:.1f}h gammal → kör collect_stats_fullseason")
            subprocess.run(["python", "-m", "src.collectors.collect_stats_fullseason"], check=True, env=env)
        else:
            print(f"[warehouse_auto] ✅ {match_path} är färsk ({age_hours:.1f}h gammal)")
    except Exception as e:
        print(f"[warehouse_auto] ⚠️ Kunde inte läsa blob-metadata ({e}) → kör collect_stats_fullseason")
        subprocess.run(["python", "-m", "src.collectors.collect_stats_fullseason"], check=True, env=env)


# ------------------------------------------------------
# 🚀 Main
# ------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Kör alla warehouse-scripts i src/warehouse/")
    args = parser.parse_args()

    today = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"[warehouse_auto] 🚀 Startar auto-warehouse {today}")

    plan_path = "src/warehouse/warehouse_plan.yaml"
    if not os.path.exists(plan_path):
        raise RuntimeError(f"Hittar inte warehouse_plan.yaml på {plan_path}")

    with open(plan_path, "r") as f:
        raw_plan = yaml.safe_load(f)

    tasks = raw_plan.get("tasks", [])

    # CLI override: kör alla Python-scripts i src/warehouse/
    if args.all:
        py_files = glob("src/warehouse/*.py")
        tasks = []
        for f in py_files:
            if f.endswith("_auto.py"):
                continue  # undvik att kalla oss själva
            module = os.path.splitext(f.replace("src/", "").replace("/", "."))[0]
            tasks.append({"job": module, "description": "auto-run", "enabled": True})
        print(f"[warehouse_auto] 🚀 Override: kör ALLA {len(tasks)} warehouse-moduler")

    # Filtrera på enabled
    enabled_tasks = [t for t in tasks if t.get("enabled", True)]
    print(f"[warehouse_auto] 📋 Jobs som ska köras: {[t['job'] for t in enabled_tasks]}")

    # 🔧 Sätt standardvärden för SEASON/LEAGUE om de inte finns
    base_env = os.environ.copy()
    base_env["SEASON"] = base_env.get("SEASON", "2025-2026")
    base_env["LEAGUE"] = base_env.get("LEAGUE", "premier_league")
    container = base_env.get("AZURE_STORAGE_CONTAINER", "afp")

    print(f"[warehouse_auto] 🌍 Kör med SEASON={base_env['SEASON']}, LEAGUE={base_env['LEAGUE']}")

    # 🧠 Säkerställ att matchdata finns innan vi kör
    ensure_fresh_matches(container, base_env["SEASON"], base_env["LEAGUE"], max_age_hours=24, env=base_env)

    # 🚀 Kör alla aktiva tasks
    for task in enabled_tasks:
        job = task["job"]
        cmd = [job]
        run(cmd, env=base_env)

    print(f"[warehouse_auto] ✅ Klar {today}")


if __name__ == "__main__":
    main()
