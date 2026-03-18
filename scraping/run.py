# scraping/run.py
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

# Import your existing modules
from .sources.lcsd_hkc import fetch_lcsd_events  # adjust if function names differ
from .sources.livenation import fetch_livenation_events  # adjust if needed
from .normalize import normalize_event  # must return a normalized dict
from .dedupe import dedupe_events       # must take list[dict] -> list[dict]

def utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def ensure_dirs() -> Dict[str, Path]:
    data_dir = Path("data")
    snapshots_dir = data_dir / "snapshots" / utc_today_str()
    data_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    return {"data": data_dir, "snap": snapshots_dir}

def to_excel(rows: List[Dict[str, Any]], path: Path) -> None:
    try:
        import pandas as pd  # pandas is in requirements.txt
    except Exception as e:
        print(f"[ERROR] pandas not available to write Excel: {e}", file=sys.stderr)
        raise

    if not rows:
        # Write an empty sheet with headers for discoverability
        df = pd.DataFrame(columns=[
            "source","id","title","venue","city","date","time","url","price_min","price_max"
        ])
    else:
        df = pd.DataFrame(rows)
    df.to_excel(path, index=False)

def main() -> int:
    paths = ensure_dirs()
    print(f"[INFO] Writing outputs under: {paths['data'].resolve()}")

    all_events: List[Dict[str, Any]] = []

    # Fetch from each source with error isolation
    sources = []
    # Adapt these callables to your real function signatures
    sources.append(("lcsd_hkc", fetch_lcsd_events))
    sources.append(("livenation", fetch_livenation_events))

    for name, fn in sources:
        try:
            print(f"[INFO] Fetching from {name} ...", flush=True)
            events = fn()  # ensure your function returns list[dict]
            print(f"[INFO] {name}: fetched {len(events)} raw events")
            all_events.extend(events)
        except Exception as e:
            print(f"[ERROR] {name} failed: {e}", file=sys.stderr)

    # Normalize
    normalized: List[Dict[str, Any]] = []
    for ev in all_events:
        try:
            normalized.append(normalize_event(ev))
        except Exception as e:
            print(f"[WARN] normalize failed for event {ev.get('id')}: {e}", file=sys.stderr)

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
    print(f"[INFO] Wrote snapshot: {snapshot_path} ({snapshot_path.stat().st_size} bytes)")

    # Write Excel summary
    excel_path = paths["data"] / "output.xlsx"
    try:
        to_excel(deduped, excel_path)
        print(f"[INFO] Wrote Excel: {excel_path} ({excel_path.stat().st_size} bytes)")
    except Exception as e:
        print(f"[ERROR] Failed writing Excel: {e}", file=sys.stderr)
        return 3

    # Non-zero exit if absolutely nothing was fetched to surface issues
    if len(deduped) == 0:
        print("[WARN] No events produced; workflow will still succeed but no data changes.", file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())