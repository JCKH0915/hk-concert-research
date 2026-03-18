# scraping/run.py
from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Best-effort import of normalization and dedupe; provide fallbacks if missing
def _resolve(name: str) -> Optional[Callable]:
    try:
        module_name, fn_name = name.rsplit(":", 1)
        mod = importlib.import_module(module_name)
        fn = getattr(mod, fn_name, None)
        return fn if callable(fn) else None
    except Exception:
        return None

normalize_event = _resolve("scraping.normalize:normalize_event")
if normalize_event is None:
    # Fallback: passthrough with light shaping
    def normalize_event(ev: Dict[str, Any]) -> Dict[str, Any]:
        """Return a minimally normalized event dict with common keys."""
        return {
            "source": ev.get("source"),
            "id": ev.get("id") or ev.get("link"),
            "title": ev.get("concert") or ev.get("title"),
            "venue": ev.get("venue"),
            "city": ev.get("city") or "Hong Kong",
            "date": (ev.get("time_iso_list") or [None])[0],
            "time": ev.get("time_text"),
            "url": ev.get("link"),
            "price_min": ev.get("price_min"),
            "price_max": ev.get("price_max"),
            "_raw": ev,  # keep the original for debugging
        }

dedupe_events = _resolve("scraping.dedupe:dedupe_events")
if dedupe_events is None:
    # Fallback: naive dedupe by (source, id or url, date)
    def dedupe_events(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out = []
        for r in rows:
            key = (r.get("source"), r.get("id") or r.get("url"), r.get("date"))
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out

# Your sources both expose fetch()
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
    cols = ["source","id","title","venue","city","date","time","url","price_min","price_max"]
    if not rows:
        df = pd.DataFrame(columns=cols)
    else:
        # Ensure consistent columns
        df = pd.DataFrame(rows)
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
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