# src/producer/produce_auto.py
from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from typing import Any, Dict
from datetime import datetime, UTC
import json

import yaml

from src.storage import azure_blob


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _to_bool(val: str | None) -> bool:
    if val is None:
        return False
    return val.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def resolve_date(plan_defaults: Dict[str, Any]) -> str:
    env_date = os.getenv("DATE")
    if env_date and env_date.strip():
        return env_date.strip()

    plan_date = None
    if isinstance(plan_defaults, dict):
        plan_date = plan_defaults.get("date")

    if isinstance(plan_date, str) and plan_date.strip():
        if plan_date.strip() == "{{today}}":
            return _now_utc().strftime("%Y-%m-%d")
        return plan_date.strip()

    return _now_utc().strftime("%Y-%m-%d")


def replace_today(obj: Any, date_str: str) -> Any:
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


def run_step(label: str, module: str, args: list[str] | None = None) -> None:
    cmd = [sys.executable, "-m", module]
    if args:
        cmd.extend(args)
    print(f"[produce_auto] Running step: {label} → {' '.join(cmd)}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"[produce_auto] ERROR: {label} failed with code {r.returncode}", file=sys.stderr)
        raise SystemExit(r.returncode)


def main() -> int:
    plan_path = os.getenv("PRODUCE_PLAN", "producer/produce_plan.yaml")
    if not Path(plan_path).exists():
        print(f"[produce_auto] ERROR: plan saknas: {plan_path}", file=sys.stderr)
        return 2
    plan = load_plan(plan_path)

    defaults = plan.get("defaults", {}) if isinstance(plan, dict) else {}
    date_str = resolve_date(defaults)
    print(f"[produce_auto] Datum: {date_str} (UTC)")

    plan = replace_today(plan, date_str)
    tasks = plan.get("tasks", [])
    if not isinstance(tasks, list) or not tasks:
        print("[produce_auto] Inga tasks i planen – avslutar.")
        return 0

    tasks.sort(key=lambda t: 0 if t.get("section_code") == "S7" else 1)
    dry_run = _to_bool(os.getenv("PRODUCE_DRY_RUN"))

    child_env = os.environ.copy()
    child_env.setdefault("WRITE_PREFIX", "producer/")
    child_env.setdefault("READ_PREFIX", "collector/")

    stats_summary = {
        "candidates": 0,
        "unique_players": 0,
        "unique_clubs": 0,
        "sections": 0,
    }

    # -----------------------------
    # STEG 1: Bygg kandidater
    # -----------------------------
    if not dry_run:
        run_step("candidates", "src.producer.produce_candidates")

    # -----------------------------
    # STEG 2: Scora kandidater
    # -----------------------------
    if not dry_run:
        run_step("scoring", "src.producer.produce_scoring")

        # Läs scored.jsonl för summering
        in_path = f"producer/candidates/{date_str}/scored.jsonl"
        try:
            text = azure_blob.get_text(os.getenv("BLOB_CONTAINER", "afp"), in_path)
            scored = [json.loads(line) for line in text.splitlines() if line.strip()]
            stats_summary["candidates"] = len(scored)
            players = {c["player"]["name"] for c in scored if c.get("player")}
            clubs = {c["player"].get("club") for c in scored if c.get("player")}
            stats_summary["unique_players"] = len(players)
            stats_summary["unique_clubs"] = len(clubs)
        except Exception as e:
            print(f"[produce_auto] WARN: kunde inte läsa scored.jsonl för summering: {e}")

    # -----------------------------
    # STEG 3: Kör tasks (som tidigare)
    # -----------------------------
    for t in tasks:
        if not isinstance(t, dict):
            continue

        section_code = t.get("section_code")
        if not section_code:
            print("[produce_auto] VARNING: task utan 'section_code' – hoppar över.")
            continue

        extra_args = t.get("args", [])
        if not isinstance(extra_args, list):
            extra_args = []

        cmd = [
            sys.executable, "-m", "src.producer.produce_section",
            "--section-code", section_code,
            "--date", date_str,
            "--path-scope", "blob",
        ]
        if extra_args:
            cmd.extend([str(a) for a in extra_args])
        if dry_run:
            cmd.append("--dry-run")

        print(f"[produce_auto] Kör sektion: {' '.join(cmd)}")
        r = subprocess.run(cmd, env=child_env)
        if r.returncode != 0:
            print(f"[produce_auto] FEL: {section_code} returnerade {r.returncode}", file=sys.stderr)
            continue

        stats_summary["sections"] += 1

    # -----------------------------
    # SLUTSUMMERING
    # -----------------------------
    print("[produce_auto] === SUMMARY ===")
    print(f"[produce_auto] Candidates: {stats_summary['candidates']}")
    print(f"[produce_auto] Unique players: {stats_summary['unique_players']}")
    print(f"[produce_auto] Unique clubs: {stats_summary['unique_clubs']}")
    print(f"[produce_auto] Sections generated: {stats_summary['sections']}")
    print("[produce_auto] DONE")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
