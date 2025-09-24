PODS_PATH = "config/pods.yaml"

def load_active_pod():
    with open(PODS_PATH, "r", encoding="utf-8") as f:
        pods_cfg = yaml.safe_load(f).get("pods", {})
    # Hitta första pod med status: on
    for pod_key, pod_cfg in pods_cfg.items():
        if str(pod_cfg.get("status", "")).lower() == "on":
            print(f"[produce_auto] Aktiv pod hittad: {pod_key}")
            return pod_key
    raise RuntimeError("[produce_auto] Ingen aktiv pod hittades i pods.yaml")

def main():
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    print(f"[produce_auto] Datum: {today} (UTC)")

    # Ladda section library
    with open(LIBRARY_PATH, "r", encoding="utf-8") as f:
        library = yaml.safe_load(f)
    sections = library.get("sections", {})
    print(f"[produce_auto] Sections i library ({len(sections)}): {', '.join(sections.keys())}")

    # Ladda aktiv pod
    pod_key = load_active_pod()

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
            "--write-latest",
            "--pod", pod_key  # från pods.yaml
        ]

        if section.startswith("S.NEWS") or section.startswith("S.OPINION"):
            cmd.extend(["--news", news_path])

        run_step(cmd, section)

    print("[produce_auto] 🎉 DONE")
