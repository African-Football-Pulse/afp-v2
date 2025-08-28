#!/usr/bin/env python3
import os, sys, argparse, importlib, json
from pathlib import Path
from datetime import datetime, UTC

try:
    import yaml
except ImportError:
    print("pip install pyyaml", file=sys.stderr); raise

def load_library(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"sections_library.yaml not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def resolve_section(library: dict, section_code: str) -> dict:
    sections = library.get("sections", {})
    if section_code not in sections:
        raise SystemExit(f"Section code not found in library: {section_code}")
    return sections[section_code]

def main():
    ap = argparse.ArgumentParser(description="Produce a section from the library (config-driven).")
    ap.add_argument("--library", default="sections_library.yaml")
    ap.add_argument("--section-code", required=True, help="e.g., S.OPINION.EXPERT_COMMENT")
    ap.add_argument("--news", required=True, help="Path to news .txt/.md")
    ap.add_argument("--date", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    ap.add_argument("--league", default="_")
    ap.add_argument("--topic", default="_")
    ap.add_argument("--speaker", default=None)          # override default_speaker
    ap.add_argument("--layout", default=None)           # override layout
    ap.add_argument("--path-scope", default=None)       # override path_scope
    ap.add_argument("--write-latest", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--outdir", default="outputs/sections")
    ap.add_argument("--personas-path", default=None)    # override personas_path
    ap.add_argument("--model", default=None)            # override model
    args = ap.parse_args()

    library = load_library(Path(args.library))
    cfg = resolve_section(library, args.section_code)

    module_name = cfg["module"]
    runner_name = cfg.get("runner", "build_section")
    mod = importlib.import_module(f"src.sections.{module_name}")
    runner = getattr(mod, runner_name)

    # merge defaults from library + CLI overrides
    opts = {
        "section_code": args.section_code,
        "news_path": args.news,
        "personas_path": args.personas_path or cfg.get("personas_path", "config/personas.json"),
        "date": args.date,
        "league": args.league,
        "topic": args.topic,
        "speaker": args.speaker or cfg.get("default_speaker"),   # may be None if not needed
        "layout": args.layout or cfg.get("layout", "alias-first"),
        "path_scope": args.path_scope or cfg.get("path_scope", "single"),
        "write_latest": args.write_latest or bool(cfg.get("write_latest", False)),
        "dry_run": args.dry_run,
        "outdir": args.outdir,
        "model": args.model or cfg.get("model") or os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        "type": cfg.get("type", "generic"),
    }

    manifest = runner(**opts)
    print(json.dumps({"ok": True, "manifest": manifest}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
