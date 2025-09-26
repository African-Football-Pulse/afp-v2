import os
import sys
import subprocess
from datetime import datetime
import yaml
from jinja2 import Environment, FileSystemLoader

def run(cmd):
    """Kör ett Pythonmodulkommando via subprocess och avbryt vid fel."""
    result = subprocess.run([sys.executable, "-m"] + cmd)
    if result.returncode != 0:
        print(f"[produce_auto] ❌ FEL: {' '.join(cmd)} misslyckades")
        sys.exit(1)

def main():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    weekday = datetime.utcnow().weekday()
    league = "premier_league"
    lang = "en"
    pod = "PL_daily_africa_en"

    print(f"[produce_auto] 🚀 Startar auto-produce för {league} {today} (weekday={weekday})")

    # 1. Kör core-producer-stegen i följd
    run(["src.producer.produce_candidates"])
    run(["src.producer.produce_scoring"])
    run(["src.producer.produce_enrich_articles"])

    # 2. Ladda produce_plan.yaml
    with open("produce_plan.yaml", "r") as f:
        plan = yaml.safe_load(f)

    # 3. Rendera episode.jinja i mode="used"
    env = Environment(loader=FileSystemLoader("templates"))
    tmpl = env.get_template("episode.jinja")
    rendered = tmpl.render(
        mode="used",
        date=today,
        league=league,
        lang=lang,
        weekday=weekday,
        sections=plan,   # används för att jinja ska kunna loopa
    )

    section_ids = [line.strip() for line in rendered.splitlines() if line.strip()]

    print(f"[produce_auto] 📋 Sektioner från template: {section_ids}")

    # 4. Kör sektionerna i turordning
    for section_id in section_ids:
        if section_id not in plan:
            print(f"[produce_auto] ⚠️ Hoppar över okänd sektion: {section_id}")
            continue

        task = plan[section_id]
        args = task.get("args", [])
        cmd = ["src.producer.produce_section", "--section", section_id] + args
        print(f"[produce_auto] ▶ Kör {section_id} → {' '.join(cmd)}")
        run(cmd)

    print("[produce_auto] ✅ Klar")

if __name__ == "__main__":
    main()
