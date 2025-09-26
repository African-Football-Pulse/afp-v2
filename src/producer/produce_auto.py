import argparse
import subprocess
import yaml
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import os
from glob import glob


def run(cmd):
    """Helper för att köra subprocess och logga"""
    print(f"[produce_auto] ▶️ Kör: {' '.join(cmd)}")
    res = subprocess.run(["python", "-m"] + cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"Fel vid körning: {' '.join(cmd)}")
    else:
        print(res.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Kör alla sektioner i src/sections/")
    args = parser.parse_args()

    today = datetime.utcnow().strftime("%Y-%m-%d")
    weekday = datetime.utcnow().weekday()
    league = "premier_league"
    lang = "en"
    pod = "PL_daily_africa_en"

    print(f"[produce_auto] 🚀 Startar auto-produce för {league} {today} (weekday={weekday})")

    # 1. Kör grundstegen
    run(["src.producer.produce_candidates", "--league", league])
    run(["src.producer.produce_scoring", "--league", league])
    run(["src.producer.produce_enrich_articles", "--league", league])

    # 2. Ladda produce_plan.yaml
    plan_path = "src/producer/produce_plan.yaml"
    if not os.path.exists(plan_path):
        raise RuntimeError(f"Hittar inte produce_plan.yaml på {plan_path}")

    with open(plan_path, "r") as f:
        raw_plan = yaml.safe_load(f)

    # Gör lookup-dict {section_id: task}
    plan = {task["section"]: task for task in raw_plan.get("tasks", [])}

    # 3. Rendera sektioner från template
    env = Environment(loader=FileSystemLoader("templates"))
    tmpl = env.get_template("episode.jinja")
    rendered = tmpl.render(
        mode="used",
        date=today,
        league=league,
        lang=lang,
        weekday=weekday,
        sections=plan,
    )
    section_ids = [line.strip() for line in rendered.splitlines() if line.strip()]

    # 4. Override: kör alla sektioner i src/sections
    if args.all:
        py_files = glob("src/sections/s_*.py")
        section_ids = [os.path.splitext(os.path.basename(f))[0].upper() for f in py_files]
        print(f"[produce_auto] 🚀 Override: kör ALLA {len(section_ids)} sektioner från src/sections/")

    print(f"[produce_auto] 📋 Sektioner som ska köras: {section_ids}")

    # 5. Kör sektionerna
    for section_id in section_ids:
        task = plan.get(section_id)
        if not task and not args.all:
            print(f"[produce_auto] ⚠️ Hoppar över okänd sektion: {section_id}")
            continue

        cmd = [
            "src.producer.produce_section",
            "--section", section_id,
            "--date", today,
            "--league", league,
            "--pod", pod,
            "--path-scope", "blob",
            "--write-latest",
        ]
        run(cmd)

    print(f"[produce_auto] ✅ Klar")


if __name__ == "__main__":
    main()
