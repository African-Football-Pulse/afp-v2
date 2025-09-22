# src/producer/produce_auto.py
import subprocess
import datetime
import yaml
import os
import sys

LIBRARY_PATH = "src/producer/sections_library.yaml"


def run_step(cmd, section):
    print(f"[produce_auto] 🚀 Startar sektion: {section}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"[produce_auto] ❌ FEL: {section} returnerade {result.returncode}")
        print(result.stderr)
        return False

    # Leta efter rad från produce_section som visar manifest
    if "[produce_section] Received manifest" in result.stdout:
        print(f"[produce_auto] ✅ Klar: {section} (manifest mottaget)")
    else:
        print(f"[produce_auto] ⚠️ VARNING: {section} gav inget manifest (fallback?)")

    # Skicka igenom stdout från subprocess för debug
    sys.stdout.write(result.stdout)
    return True


def main():
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    print(f"[produce_auto] Datum: {today} (UTC)")

    # Ladda section library
    with open(LIBRARY_PATH, "r", encoding="utf-8") as f:
        library = yaml.safe_load(f)
    sections = library.get("sections", {})
    print(f"[produce_auto] Sections i library ({len(sections)}): {', '.join(sections.keys())}")

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

    # Path till dagens scored.jsonl
    news_path = f"producer/candidates/{today}/scored.jsonl"

    # 3. Kör alla sektioner
    for section, cfg in sections.items():
        cmd = [
            sys.executable, "-m", "src.producer.produce_section",
            "--section", section,
            "--date", today,
            "--path-scope", "blob",
            "--league", "premier_league",
            "--outdir", "sections",
            "--write-latest"
        ]

        # NEWS och OPINION behöver kandidater
        if section.startswith("S.NEWS") or section.startswith("S.OPINION"):
            cmd.extend(["--news", news_path])

        run_step(cmd, section)

    print("[produce_auto] 🎉 DONE")


if __name__ == "__main__":
    main()
