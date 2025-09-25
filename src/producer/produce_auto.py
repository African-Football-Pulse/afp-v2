# src/producer/produce_auto.py
import os
import sys
import subprocess
import datetime
import yaml

LIBRARY_PATH = "config/sections_library.yaml"
PODS_PATH = "config/pods.yaml"


def run_step(cmd, section):
    print(f"[produce_auto] ▶ Kör sektion: {section}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"[produce_auto] ❌ FEL i sektion: {section}")
    else:
        print(f"[produce_auto] ✅ Klar: {section}")


def load_active_pod():
    with open(PODS_PATH, "r", encoding="utf-8") as f:
        pods_cfg = yaml.safe_load(f).get("pods", {})
    for pod_key, pod_cfg in pods_cfg.items():
        if str(pod_cfg.get("status", "")).lower() == "on":
            print(f"[produce_auto] Aktiv pod hittad: {pod_key}")
            return pod_key, pod_cfg
    raise RuntimeError("[produce_auto] Ingen aktiv pod hittades i pods.yaml")


def main():
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    weekday = datetime.datetime.utcnow().weekday()
    print(f"[produce_auto] Datum: {today} (UTC), Weekday={weekday}")

    # Ladda section library
    with open(LIBRARY_PATH, "r", encoding="utf-8") as f:
        library = yaml.safe_load(f)
    sections = library.get("sections", {})
    print(f"[produce_auto] Sections i library ({len(sections)}): {', '.join(sections.keys())}")

    # Ladda aktiv pod
    pod_key, pod_cfg = load_active_pod()
    leagues = pod_cfg.get("leagues", ["premier_league"])
    langs = pod_cfg.get("langs", ["en"])
    league = leagues[0]  # första ligan i listan
    lang = langs[0]      # första språket i listan

    # 1. Produce candidates
    result = subprocess.run([sys.executable, "-m", "src.producer.produce_candidates"])
    if result.returncode != 0:
        print("[produce_auto] ❌ FEL: produce_candidates misslyckades")
        return

    # 2. Produce scoring
    result = subprocess.run([sys.executable, "-m", "src.producer.produce_scoring"])
    if result.returncode != 0:
        print("[produce_auto] ❌ FEL: produce_scoring misslyckades")
        return

    # Path till dagens scored.jsonl (global, inte per liga)
    news_path = f"producer/scored/{today}/scored.jsonl"

    # 3. Bestäm sektioner för dagens podd
    if weekday == 0:  # måndag → postmatch
        sections_to_run = ["S.GENERIC.INTRO_POSTMATCH"]
        sections_to_run += [s for s in sections if s.startswith("S.NEWS.")]
    elif weekday == 1:  # tisdag → daily + stats
        sections_to_run = ["S.GENERIC.INTRO_DAILY", "S.NEWS.TOP3", "S.STATS.TOP_AFRICAN_PLAYERS"]
    elif weekday == 3:  # torsdag → daily + highlight + expert
        sections_to_run = ["S.GENERIC.INTRO_DAILY", "S.NEWS.HIGHLIGHT", "S.OPINION.EXPERT_COMMENT"]
    else:  # övriga dagar → daily + top3
        sections_to_run = ["S.GENERIC.INTRO_DAILY", "S.NEWS.TOP3"]

    print(f"[produce_auto] Sektioner som ska köras idag: {sections_to_run}")

    # 4. Kör valda sektioner
    for section in sections_to_run:
        cmd = [
            sys.executable, "-m", "src.producer.produce_section",
            "--section", section,
            "--date", today,
            "--path-scope", "blob",
            "--league", league,
            "--outdir", "sections",
            "--write-latest",
            "--pod", pod_key,
            "--lang", lang,
        ]

        if section.startswith("S.NEWS") or section.startswith("S.OPINION"):
            cmd.extend(["--news", news_path])

        run_step(cmd, section)

    print("[produce_auto] 🎉 DONE")


if __name__ == "__main__":
    main()
