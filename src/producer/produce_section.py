# src/producer/produce_section.py
import os
import sys
import argparse
import importlib
import traceback
import json
import inspect
from pathlib import Path

import yaml

# Default path till sections_library.yaml (nu flyttad till producer/)
DEFAULT_LIBRARY_FILE = "src/producer/sections_library.yaml"


def load_library(path: str = DEFAULT_LIBRARY_FILE):
    if not Path(path).exists():
        raise RuntimeError(f"Sections library saknas: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def build_section(section_code: str, args: argparse.Namespace, library: dict):
    section = library.get("sections", {}).get(section_code)
    if not section:
        raise RuntimeError(f"Section code '{section_code}' hittades ej i library")

    module_name = section.get("module")
    runner = section.get("runner", "build_section")

    if not module_name:
        raise RuntimeError(f"Section '{section_code}' saknar modul i library")

    # Importera modul från src/sections/
    mod_path = f"src.sections.{module_name}"
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise RuntimeError(f"Kunde inte importera modul {mod_path}: {e}")

    if not hasattr(mod, runner):
        raise RuntimeError(f"Modulen {mod_path} saknar funktionen {runner}")

    fn = getattr(mod, runner)

    # Flexibelt: stöd för både 0 och 1 argument
    sig = inspect.signature(fn)
    if len(sig.parameters) == 0:
        return fn()
    else:
        return fn(args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--section-code", required=True, help="Kod från sections_library.yaml")
    parser.add_argument("--date", required=True, help="Datum (YYYY-MM-DD)")
    parser.add_argument("--path-scope", default="blob", help="Datakälla (blob/local)")
    parser.add_argument("--league", default="premier_league")
    parser.add_argument("--outdir", default="sections")
    parser.add_argument("--write-latest", action="store_true")
    parser.add_argument("--news", nargs="*")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    try:
        library = load_library()
        section_obj = build_section(args.section_code, args, library)

        if args.dry_run:
            print(json.dumps(section_obj, ensure_ascii=False, indent=2))
            return 0

        # Skriv ut till stdout + fil
        out_dir = Path(args.outdir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{args.section_code}_{args.date}.json"

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(section_obj, f, ensure_ascii=False, indent=2)

        if args.write_latest:
            latest_file = out_dir / f"{args.section_code}_latest.json"
            with open(latest_file, "w", encoding="utf-8") as f:
                json.dump(section_obj, f, ensure_ascii=False, indent=2)

        print(f"[produce_section] Wrote {out_file}")
        return 0

    except Exception as e:
        print(f"[produce_section] ERROR: {e}", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
