# scraping/run.py
from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .normalize import normalize_event
from .dedupe import dedupe_events

# Your modules and the exact fetch function they expose
SOURCES = [
    ("scraping.sources.lcsd_hkc", "fetch"),
    ("scraping.sources.livenation", "fetch"),
]

def utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def ensure_dirs() -> Dict[str, Path]:
    data_dir = Path("data")
    snapshots_dir = data_dir / "snapshots" / utc_today_str()
    data_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    return {"data": data_dir, "snap": snapshots_dir}

def import_fetch(module_path: str, fn_name: str) -> Optional[Callable[[], List[Dict[str, Any]]]]:
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        print(f"[ERROR] import {module_path} failed: {e}", file=sys.stderr)
        return None
    fn = getattr(mod, fn_name, None)
    if not callable(fn):
        print(f"[ERROR] {module_path} has no callable {fn_name}()", file=sys.stderr)
        return None
    return fn

def to_excel(rows: List[Dict[str, Any]], path: Path) -> None:
    import pandas as pd
    if not rows:
        df = pd.DataFrame(columns=[
            "source","id","title","venue","city","date","time","url","price_min","price_max"
        ])
    else:
        df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False)

def main() -> int:
    paths = ensure_dirs()
    print(f"[INFO] Output directory: {paths['data'].resolve()}")

    all_events: List[Dict[str, Any]] = []

    for module_path, fn_name in SOURCES:
        fn = import_fetch(module_path, fn_name)
        if not fn:
            continue
        try:
            print(f"[INFO] Fetching via {module_path}.{fn_name}() ...", flush=True)
            result = fn()
            events = result if isinstance(result, list) else result.get("events", []) if isinstance(result, dict) else []
            print(f"[INFO] {module_path}: fetched {len(events)} raw events")
            all_events.extend(events)
        except Exception as e:
            print(f"[ERROR] {module_path}.{fn_name} failed: {e}", file=sys.stderr)

    # Normalize
    normalized: List[Dict[str, Any]] = []
    for ev in all_events:
        try:
            normalized.append(normalize_event(ev))
        except Exception as e:
            print(f"[WARN] normalize failed for one event: {e}", file=sys.stderr)

    print(f"[INFO] Normalized: {len(normalized)}")

    # Dedupe
    try:
        deduped = dedupe_events(normalized)
    except Exception as e:
        print(f"[ERROR] dedupe failed: {e}", file=sys.stderr)
        return 2

    print(f"[INFO] Deduped: {len(deduped)}")

    # Snapshot JSON
    snapshot_path = paths["snap"] / "raw.json"
    with snapshot_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "counts": {
                    "raw": len(all_events),
                    "normalized": len(normalized),
                    "deduped": len(deduped),
                },
                "events": deduped,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"[INFO] Wrote snapshot: {snapshot_path}")

    # Excel
    excel_path = paths["data"] / "output.xlsx"
    try:
        to_excel(deduped, excel_path)
        print(f"[INFO] Wrote Excel: {excel_path}")
    except Exception as e:
        print(f"[ERROR] writing Excel failed: {e}", file=sys.stderr)
        return 3

    if len(deduped) == 0:
        print("[WARN] No events produced; sources may have filtered out non-HK events.", file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())