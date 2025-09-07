from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from typing import Any, Dict
from datetime import datetime

import yaml

# För Blob-nedladdning av --news-filer (om paketet finns i imagen)
try:
    from azure.storage.blob import BlobServiceClient  # type: ignore
except Exception:
    BlobServiceClient = None  # fortsätt utan auto-hämtning


# -----------------------------
# Hjälpfunktioner
# -----------------------------

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # Fallback om zoneinfo saknas

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


def _materialize_news_args(
    args_list: list[str],
    read_prefix: str,
    container: str,
    conn_str: str,
) -> list[str]:
    """
    För varje '--news <relpath>':
      - Om filen <relpath> saknas lokalt:
          hämta 'read_prefix + relpath' från Blob och spara lokalt på <relpath>.
      - Returnerar oförändrad args_list (vägarna pekar nu på lokala filer som existerar).
    Kräver: azure-storage-blob och giltig AZURE_STORAGE_CONNECTION_STRING + AZURE_CONTAINER.
    """
    if not args_list:
        return args_list

    if BlobServiceClient is None:
        # Paket saknas – hoppa över autohämtning
        return args_list

    if not container or not conn_str:
        # Saknar anslutningsdata – låt runner signalera ev. filfel
        return args_list

    try:
        svc = BlobServiceClient.from_connection_string(conn_str)
    except Exception as e:
        print(f"[produce_auto] WARN: Kunde inte initiera BlobServiceClient: {e}")
        return args_list

    def download_to_local(blob_path: str, local_path: str) -> None:
        try:
            bc = svc.get_blob_client(container=container, blob=blob_path)
            data = bc.download_blob().readall()
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(data)
            print(f"[produce_auto] Fetched news to local: {local_path} <- {blob_path}")
        except Exception as e:
            print(f"[produce_auto] WARN: Could not fetch {blob_path}: {e}")

    i = 0
    while i < len(args_list):
        if args_list[i] == "--news" and i + 1 < len(args_list):
            relpath = str(args_list[i + 1])
            if not os.path.exists(relpath):
                # Bygg blob-sökvägen: READ_PREFIX + relpath (READ_PREFIX bör vara t.ex. 'collector/')
                blob_path = f"{read_prefix}{relpath}"
                download_to_local(blob_path, relpath)
            i += 2
        else:
            i += 1

    return args_list


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

    # Blob-anslutning för ev. autohämtning
    conn_str = child_env.get("AZURE_STORAGE_CONNECTION_STRING", "")
    container = child_env.get("AZURE_CONTAINER", "")
    read_prefix = child_env.get("READ_PREFIX", "collector/")

    # 7) Kör alla tasks
    for t in tasks:
        if not isinstance(t, dict):
            continue

        section_code = t.get("section_code")
        if not section_code:
            print("[produce_auto] VARNING: task utan 'section_code' – hoppar över.")
            continue

        # Extra args från planen (redan '{{today}}' ersatt om de fanns)
        extra_args = t.get("args", [])
        if not isinstance(extra_args, list):
            extra_args = []

        # Autohämta alla --news-filer till lokalt fs om de saknas
        extra_args = _materialize_news_args(extra_args, read_prefix, container, conn_str)

        # Bygg kommandot till produce_section
        cmd = [
            sys.executable, "-m", "src.produce_section",
            "--section-code", section_code,
            "--date", date_str,
            "--path-scope", "blob",  # skriv direkt till Blob
        ]
        if extra_args:
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
