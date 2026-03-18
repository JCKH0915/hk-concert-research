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

COMMON_FN_NAMES = ("fetch_events", "fetch", "get_events", "scrape", "run", "main")

def utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def ensure_dirs() -> Dict[str, Path]:
    data_dir = Path("data")
    snapshots_dir = data_dir / "snapshots" / utc_today_str()
    data_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    return {"data": data_dir, "snap": snapshots_dir}

def resolve_fetch_fn(module_path: str) -> Optional[Callable[[], List[Dict[str, Any]]]]:
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        print(f"[ERROR] Failed importing {module_path}: {e}", file=sys.stderr)
        return None
    for name in COMMON_FN_NAMES:
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn
    print(f"[ERROR] No suitable fetch function found in {module_path}. "
          f"Tried: {', '.join(COMMON_FN_NAMES)}", file=sys.stderr)
    return None

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
    print(f"[INFO] Writing outputs under: {paths['data'].resolve()}")

    sources = [
        "scraping.sources.lcsd_hkc",
        "scraping.sources.livenation",
    ]

    all_events: List[Dict[str, Any]] = []

    for module_path in sources:
        fn = resolve_fetch_fn(module_path)
        if not fn:
            continue
        try:
            print(f"[INFO] Fetching from {module_path} via {fn.__name__}() ...", flush=True)
            result = fn()
            if result is None:
                count = 0
                events = []
            elif isinstance(result, list):
                events = result
                count = len(events)
            else:
                # If function returned a dict with 'events'
                events = result.get("events", []) if isinstance(result, dict) else []
                count = len(events)
            print(f"[INFO] {module_path}: fetched {count} raw events")
            all_events.extend(events)
        except Exception as e:
            print(f"[ERROR] {module_path} failed: {e}", file=sys.stderr)

    # Normalize
    normalized: List[Dict[str, Any]] = []
    for ev in all_events:
        try:
            normalized.append(normalize_event(ev))
        except Exception as e:
            print(f"[WARN] normalize failed for event {ev}: {e}", file=sys.stderr)

    print(f"[INFO] Normalized events: {len(normalized)}")

    # Dedupe
    try:
        deduped = dedupe_events(normalized)
    except Exception as e:
        print(f"[ERROR] dedupe failed: {e}", file=sys.stderr)
        return 2

    print(f"[INFO] Deduped events: {len(deduped)}")

    # Write snapshot JSON
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

    # Write Excel summary
    excel_path = paths["data"] / "output.xlsx"
    try:
        to_excel(deduped, excel_path)
        print(f"[INFO] Wrote Excel: {excel_path}")
    except Exception as e:
        print(f"[ERROR] Failed writing Excel: {e}", file=sys.stderr)
        return 3

    if len(deduped) == 0:
        print("[WARN] No events produced; check source modules or site availability.", file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())