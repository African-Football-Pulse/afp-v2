#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json
from pathlib import Path
from datetime import datetime, timezone

try:
    import yaml
except ImportError:
    print("PyYAML saknas. Kör: pip install pyyaml", file=sys.stderr)
    raise

HERE = Path(__file__).resolve()
SRC = HERE.parent
ROOT = SRC.parent
PLAN_PATH = ROOT / "config" / "produce_plan.yaml"

def log(msg: str):  # lite pratsam men tydlig i ACA-loggar
    print(f"[produce_auto] {msg}", flush=True)

def load_yaml(p: Path):
    if not p.exists():
        raise SystemExit(f"produce_plan.yaml saknas: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}

def today_ymd(tz="Europe/Stockholm"):
    try:
        from zoneinfo import ZoneInfo
        z = ZoneInfo(tz)
    except Exception:
        z = timezone.utc
    return datetime.now(z).strftime("%Y-%m-%d")

def normalize_section(code: str) -> str:
    sc = (code or "").strip().upper()
    mapping = {"S.NEWS.TOPN": "S.NEWS.TOP3", "TOPN": "S.NEWS.TOP3", "TOP3": "S.NEWS.TOP3"}
    return mapping.get(sc, (code or "").strip())

def build_news_url(source: str, league: str, date: str) -> str:
    """
    Standard Collect-path: curated/news/<source>/<league>/<date>/items.json
    - Finns BLOB_CONTAINER_SAS_URL -> retur https-URL inkl. SAS.
    - Annars -> lokal container-path (/app/local_out/...).
    """
    source = (source or "").strip()
    league = (league or "").strip()
    date = (date or "").strip()

    rel = f"curated/news/{source}/{league}/{date}/items.json"

    sas = os.getenv("BLOB_CONTAINER_SAS_URL", "").strip()
    if sas:
        base, _, query = sas.partition("?")  # base: https://<acct>.blob.core.windows.net/<container>
        base = base.rstrip("/")
        # om base redan slutar med /afp (eller annan container), append rel direkt
        if base.count("/") >= 3:
            return f"{base}/{rel}?{query}"
        # defensivt fallback (bör ej hända)
        return f"{base}/afp/{rel}?{query}"
    else:
        return f"/app/local_out/{rel}"

def run_one(section_code: str,
            news_url: str,
            outdir: str,
            date: str,
            league: str,
            layout: str | None,
            path_scope: str | None,
            personas_path: str | None,
            model: str | None,
            speaker: str | None,
            write_latest: bool):
    """
    Kör din befintliga produce_section genom att sätta sys.argv och kalla main().
    Ingen dependency på interna funktioner.
    """
    section_code = normalize_section(section_code)

    argv = [
        "produce_section.py",
        "--section-code", section_code,
        "--news", news_url,
        "--date", date,
        "--league", league,
        "--outdir", outdir,
    ]
    if layout:        argv += ["--layout", layout]
    if path_scope:    argv += ["--path-scope", path_scope]
    if personas_path: argv += ["--personas-path", personas_path]
    if model:         argv += ["--model", model]
    if speaker:       argv += ["--speaker", speaker]
    if write_latest:  argv += ["--write-latest"]

    log(f"RUN section={section_code} league={league} date={date}")
    # Importera och kör main()
    import importlib
    ps = importlib.import_module("src.produce_section")
    old_argv = sys.argv[:]
    try:
        sys.argv = argv
        ps.main()
        return {"ok": True}
    finally:
        sys.argv = old_argv

def main():
    plan = load_yaml(PLAN_PATH)

    defaults = plan.get("defaults", {}) or {}
    date = (defaults.get("date") or "").strip() or today_ymd()
    league = defaults.get("league", "premier_league")
    outdir = defaults.get("outdir", "/app/blob_out/produce")
    layout = defaults.get("layout")
    path_scope = defaults.get("path_scope")
    personas_path = defaults.get("personas_path", "config/personas.json")
    model = defaults.get("model")
    speaker = defaults.get("speaker")
    write_latest = bool(defaults.get("write_latest", True))

    tasks = plan.get("tasks", []) or []
    if not tasks:
        log("Inga tasks i produce_plan.yaml — avslutar 0.")
        print(json.dumps({"ok": True, "results": []}, ensure_ascii=False))
        return

    results = []
    for t in tasks:
        sec = t.get("section_code")
        src = t.get("source")
        lg  = t.get("league", league)
        dt  = (t.get("date") or "").strip() or date

        news = (t.get("news") or "").strip() or build_news_url(src, lg, dt)
        res = run_one(
            section_code = sec,
            news_url     = news,
            outdir       = t.get("outdir", outdir),
            date         = dt,
            league       = lg,
            layout       = t.get("layout", layout),
            path_scope   = t.get("path_scope", path_scope),
            personas_path= t.get("personas_path", personas_path),
            model        = t.get("model", model),
            speaker      = t.get("speaker", speaker),
            write_latest = t.get("write_latest", write_latest),
        )
        results.append({"section": normalize_section(sec), "news": news, "result": res})

    print(json.dumps({"ok": True, "results": results}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
