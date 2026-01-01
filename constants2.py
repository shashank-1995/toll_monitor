from __future__ import annotations

import json
import urllib.request
from datetime import datetime
from pathlib import Path

from config import CONFIG


def _fetch(url: str, dst: Path, timeout_sec: int = 8) -> None:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "toll-audit/1.0", "Cache-Control": "no-cache"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        data = resp.read()

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".tmp")
    tmp.write_bytes(data)
    tmp.replace(dst)


def _load_payload() -> dict:
    cache_path = CONFIG.constants_cache_path

    # Fetch fresh constants on startup (best-effort)
    if CONFIG.constants_url:
        try:
            _fetch(CONFIG.constants_url, cache_path)
        except Exception:
            # If fetch fails, try using existing local cache
            pass

    if not cache_path.exists():
        raise RuntimeError(
            f"Missing constants cache: {cache_path}. "
            "Set CONFIG.constants_url or place constants_cache.json manually."
        )

    return json.loads(cache_path.read_text(encoding="utf-8"))


_payload = _load_payload()

same_day = _payload["same_day"]
paired_tolls = [tuple(x) for x in _payload["paired_tolls"]]
lst_rj = _payload["lst_rj"]
consolidated_exp_tolls = _payload.get("consolidated_exp_tolls", [])
dd_t = _payload.get("dd_t", 29)

dt = datetime.strptime(_payload["dt"], "%Y-%m-%d %H:%M:%S")

neighbour_tolls = _payload.get("neighbour_tolls", [])
delt_lst = _payload.get("delt_lst", [])
park_tolls = _payload.get("park_tolls", [])
