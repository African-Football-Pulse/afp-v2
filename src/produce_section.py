#!/usr/bin/env python3
import os, sys, argparse, importlib, json, inspect
from pathlib import Path
from datetime import datetime, UTC

try:
    import yaml
except ImportError:
    print("PyYAML saknas. Kör: pip install pyyaml", file=sys.stderr)
    raise

# --- funkar både "python -m src.produce_section" och "python src/produce_section.py"
THIS_FILE = Path(__file__).resolve()
SRC_DIR = THIS_FILE.parent
REPO_ROOT = SRC_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))  # lägg till projektroten

DEFAULT_LIBRARY = "config/sections_library.yaml"

def load_library(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"sections_library.yaml not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def resolve_section(library: dict, section_code: str) -> dict:
    sections = library.get("sections", {})
    if section_code not in sections:
        raise SystemExit(f"Section code not found in library: {section_code}")
    return sections[section_code]

def import_section_module(module_name: str):
    """
    Ladda sektionmodulen direkt från fil: src/sections/<module_name>.py.
    Om filen inte finns → fallback till paketimport.
    """
    import importlib.util
    from importlib.machinery import SourceFileLoader

    candidate = SRC_DIR / "sections" / f"{module_name}.py"
    tried = []

    if candidate.exists():
        try:
            spec = importlib.util.spec_from_file_location(
                f"afp.sections.{module_name}",
                str(candidate),
                loader=SourceFileLoader(f"afp.sections.{module_name}", str(candidate))
            )
            if spec is None or spec.loader is None:
                raise ModuleNotFoundError(f"Kunde inte skapa spec för: {candidate}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # kör filen deterministiskt
            print(f"[produce_section] Loaded module from file: {candidate}")
            return mod
        except Exception as e:
            tried.append((str(candidate), repr(e)))

    # Fallback: prova paketimport (om man kör som modul och vill använda paket)
    for fq in (f"src.sections.{module_name}", f"sections.{module_name}"):
        try:
            mod = importlib.import_module(fq)
            print(f"[produce_section] Loaded module via package import: {fq} (file={getattr(mod,'__file__','<unknown>')})")
            return mod
        except ModuleNotFoundError as e:
            tried.append((fq, str(e)))
            continue

    msg = "Kunde inte importera sektionmodulen.\n"
    for where, err in tried:
        msg += f"- {where}: {err}\n"
    raise ModuleNotFoundError(msg)

def main():
    ap = argparse.ArgumentParser(description="Produce a section from the library (config-driven).")
    ap.add_argument("--library", default=DEFAULT_LIBRARY)
    ap.add_argument("--section-code", required=True, help="t.ex. S.OPINION.EXPERT_COMMENT")
    ap.add_argument("--news", required=False, help="Path till nyhetsunderlag (.txt/.md)")  # ändrat till optional
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

    # --- Guard: normalisera TOPN/TOP3 till kanoniskt namn
    sc_norm = args.section_code.strip().upper()
    mapping = {
        "S.NEWS.TOPN": "S.NEWS.TOP3",
        "TOPN": "S.NEWS.TOP3",
        "TOP3": "S.NEWS.TOP3"
    }
    if sc_norm in mapping:
        print(f"[produce_section] Normaliserar section-code {args.section_code} → {mapping[sc_norm]}")
        args.section_code = mapping[sc_norm]

    library = load_library(Path(args.library))
    cfg = resolve_section(library, args.section_code)

    module_name = cfg["module"]
    runner_name = cfg.get("runner", "build_section")
    mod = import_section_module(module_name)
    if not hasattr(mod, runner_name):
        raise SystemExit(f"Runner '{runner_name}' saknas i modul '{module_name}'")
    runner = getattr(mod, runner_name)
    
    # --- merge defaults from library + CLI overrides
    speaker_val = args.speaker or cfg.get("default_speaker")
    raw_opts = {
        "section_code": args.section_code,
        "news_path": args.news,  # kan nu vara None
        "personas_path": args.personas_path or cfg.get("personas_path", "config/personas.json"),
        "date": args.date,
        "league": args.league,
        "topic": args.topic,
        "layout": args.layout or cfg.get("layout", "alias-first"),
        "path_scope": args.path_scope or cfg.get("path_scope"),
        "write_latest": args.write_latest or bool(cfg.get("write_latest", False)),
        "dry_run": args.dry_run,
        "outdir": args.outdir,
        "model": args.model or cfg.get("model") or os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        "type": cfg.get("type", "generic"),
        "speaker": speaker_val,
    }

    sig = inspect.signature(runner)
    accepted = set(sig.parameters.keys())
    opts = {k: v for k, v in raw_opts.items() if (k in accepted and v is not None)}

    manifest = runner(**opts)
    print(json.dumps({"ok": True, "manifest": manifest}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
