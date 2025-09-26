import argparse
import subprocess
import yaml
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import os


def run(cmd):
    """Helper to köra subprocess och logga"""
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
    parser.add_argument("--all-sections", action="store_true", help="Kör alla sektioner i sections_library.yaml")
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

    if args.all_sections:
        section_ids = list(plan.keys())
        print(f"[produce_auto] 🚀 Override: kör ALLA {len(section_ids)} sektioner")

    print(f"[produce_auto] 📋 Sektioner från template: {section_ids}")

    # 4. Kör sektionerna
    for section_id in section_ids:
        task = plan.get(section_id)
        if not task:
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
