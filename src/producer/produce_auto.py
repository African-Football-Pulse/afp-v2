import os
import sys
import subprocess
import yaml
from datetime import datetime, timezone

def load_plan(path="src/producer/produce_plan.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    today = datetime.now(timezone.utc).date().isoformat()
    plan_path = os.getenv("PRODUCE_PLAN", "src/producer/produce_plan.yaml")

    try:
        plan = load_plan(plan_path)
    except Exception as e:
        print(f"[produce_auto] ERROR: plan saknas: {plan_path}")
        sys.exit(1)

    defaults = plan.get("defaults", {})
    league = defaults.get("league", "premier_league")

    print(f"[produce_auto] Datum: {today} (UTC)")

    # Step 1: kör candidates
    cmd_candidates = [sys.executable, "-m", "src.producer.produce_candidates"]
    print(f"[produce_auto] Running step: candidates → {' '.join(cmd_candidates)}")
    rc = subprocess.call(cmd_candidates)
    if rc != 0:
        print(f"[produce_auto] ERROR: candidates failed with code {rc}")
        sys.exit(rc)

    # Step 2: kör scoring
    cmd_scoring = [sys.executable, "-m", "src.producer.produce_scoring"]
    print(f"[produce_auto] Running step: scoring → {' '.join(cmd_scoring)}")
    rc = subprocess.call(cmd_scoring)
    if rc != 0:
        print(f"[produce_auto] ERROR: scoring failed with code {rc}")
        sys.exit(rc)

    # Step 3: kör sektioner
    for task in plan.get("tasks", []):
        section = task.get("section")
        if not section:
            print(f"[produce_auto] SKIP: task saknar 'section'")
            continue

        args = task.get("args", [])
        cmd = [
            sys.executable, "-m", "src.producer.produce_section",
            "--section", section,
            "--date", today,
            "--path-scope", "blob",
            "--league", league,
            "--outdir", "sections"
        ] + args

        if defaults.get("write_latest", False) and "--write-latest" not in cmd:
            cmd.append("--write-latest")

        print(f"[produce_auto] Kör sektion: {' '.join(cmd)}")
        rc = subprocess.call(cmd)
        if rc != 0:
            print(f"[produce_auto] FEL: {section} returnerade {rc}")

    print("[produce_auto] DONE")

if __name__ == "__main__":
    main()
