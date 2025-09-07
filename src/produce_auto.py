from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from typing import Any, Dict
from azure.storage.blob import BlobServiceClient

import yaml
from datetime import datetime

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # Fallback om zoneinfo saknas


# -----------------------------
# Hjälpfunktioner
# -----------------------------

TZ_STOCKHOLM = ZoneInfo("Europe/Stockholm") if ZoneInfo else None


def _now_stockholm() -> datetime:
    """Ge nuvarande tid i Europe/Stockholm (eller naiv now som fallback)."""
    if TZ_STOCKHOLM:
        return datetime.now(TZ_STOCKHOLM)
    return datetime.now()


def _to_bool(val: str | None) -> bool:
    """Tolkar vanliga sanningsvärden i env-variabler."""
    if val is None:
        return False
    return val.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def resolve_date(plan_defaults: Dict[str, Any]) -> str:
    """
    Prioritet för datum:
    1) ENV var 'DATE'
    2) plan.defaults.date (tillåter '{{today}}')
    3) dagens datum i Europe/Stockholm
    """
    env_date = os.getenv("DATE")
    if env_date and env_date.strip():
        return env_date.strip()

    plan_date = None
    if isinstance(plan_defaults, dict):
        plan_date = plan_defaults.get("date")

    if isinstance(plan_date, str) and plan_date.strip():
        if plan_date.strip() == "{{today}}":
            return _now_stockholm().strftime("%Y-%m-%d")
        # anta redan ett giltigt datumformat (YYYY-MM-DD)
        return plan_date.strip()

    # fallback: dagens datum
    return _now_stockholm().strftime("%Y-%m-%d")


def replace_today(obj: Any, date_str: str) -> Any:
    """
    Ersätter strängen '{{today}}' med 'date_str' i hela objektet (rekursivt).
    Hanterar dict, listor och strängar.
    """
    if isinstance(obj, str):
        return obj.replace("{{today}}", date_str)
    if isinstance(obj, list):
        return [replace_today(x, date_str) for x in obj]
    if isinstance(obj, dict):
        return {k: replace_today(v, date_str) for k, v in obj.items()}
    return obj


def load_plan(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# -----------------------------
# Huvudflöde
# -----------------------------

def main() -> int:
    # 1) Läs in plan
    plan_path = os.getenv("PRODUCE_PLAN", "config/produce_plan.yaml")
    if not Path(plan_path).exists():
        print(f"[produce_auto] ERROR: plan saknas: {plan_path}", file=sys.stderr)
        return 2
    plan = load_plan(plan_path)

    # 2) Lös datum
    defaults = plan.get("defaults", {}) if isinstance(plan, dict) else {}
    date_str = resolve_date(defaults)
    print(f"[produce_auto] Datum: {date_str}")

    # 3) Ersätt alla '{{today}}' i hela planen
    plan = replace_today(plan, date_str)

    # 4) Läs tasks
    tasks = plan.get("tasks", [])
    if not isinstance(tasks, list) or not tasks:
        print("[produce_auto] Inga tasks i planen – avslutar.")
        return 0

    # 5) Dry-run: endast om man explicit ber om det
    dry_run = _to_bool(os.getenv("PRODUCE_DRY_RUN"))
    if dry_run:
        print("[produce_auto] PRODUCE_DRY_RUN=true → kör med --dry-run")

    # 6) Barnprocessens miljö: säkerställ prefix-standarder
    child_env = os.environ.copy()
    child_env.setdefault("WRITE_PREFIX", "producer/")
    child_env.setdefault("READ_PREFIX", "collector/")

    # 7) Kör alla tasks
    for t in tasks:
        if not isinstance(t, dict):
            continue

        section_code = t.get("section_code")
        if not section_code:
            print("[produce_auto] VARNING: task utan 'section_code' – hoppar över.")
            continue

        # Bygg kommandot till produce_section

        cmd = [
            sys.executable, "-m", "src.produce_section",
            "--section-code", section_code,
            "--date", date_str,
            "--path-scope", "blob",
        ]
        # Extra args från planen (redan '{{today}}' ersatt om de fanns)
        extra_args = t.get("args", [])
        if isinstance(extra_args, list) and extra_args:
            cmd.extend([str(a) for a in extra_args])

        if dry_run:
            cmd.append("--dry-run")

        print(f"[produce_auto] Kör: {' '.join(cmd)}")
        r = subprocess.run(cmd, env=child_env)
        if r.returncode != 0:
            print(f"[produce_auto] FEL: {section_code} returnerade {r.returncode}", file=sys.stderr)
            return r.returncode

    print("[produce_auto] Klart – alla tasks körda.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
