import argparse
import subprocess
import yaml
from datetime import datetime
import os
from glob import glob


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

    print(f"[warehouse_auto] 🌍 Kör med SEASON={base_env['SEASON']}, LEAGUE={base_env['LEAGUE']}")

    for task in enabled_tasks:
        job = task["job"]
        cmd = [job]
        run(cmd, env=base_env)

    print(f"[warehouse_auto] ✅ Klar {today}")


if __name__ == "__main__":
    main()
