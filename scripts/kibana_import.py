"""Import the repo-stored Kibana saved objects idempotently (overwrite by id) and verify.

Objects: data view over the `product_month` alias, three Lens panels, one dashboard.
Prints `KIBANA_DASHBOARD ... present=true|false`. Exit 1 if the import reports an error.

Run:  ./run.sh python scripts/kibana_import.py [--host http://localhost:5601]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

from src.common import config as C

NDJSON = C.PROJECT_ROOT / "conf" / "kibana" / "product_month_dashboard.ndjson"
DASHBOARD_ID = "pm-dashboard"
KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")


def _req(host: str, path: str, *, method: str = "GET", body: bytes | None = None,
         content_type: str | None = None) -> dict:
    req = urllib.request.Request(host + path, data=body, method=method)
    req.add_header("kbn-xsrf", "true")
    if content_type:
        req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def import_objects(host: str, path: Path) -> dict:
    boundary = "----review-radar"
    payload = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{path.name}\"\r\n"
               f"Content-Type: application/ndjson\r\n\r\n").encode() + path.read_bytes() + \
              f"\r\n--{boundary}--\r\n".encode()
    return _req(host, "/api/saved_objects/_import?overwrite=true", method="POST", body=payload,
                content_type=f"multipart/form-data; boundary={boundary}")


def dashboard_present(host: str) -> bool:
    try:
        obj = _req(host, f"/api/saved_objects/dashboard/{DASHBOARD_ID}")
    except Exception:  # noqa: BLE001
        return False
    return obj.get("id") == DASHBOARD_ID and not obj.get("error")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=KIBANA_HOST)
    args = ap.parse_args()
    result = import_objects(args.host, NDJSON)
    errors = result.get("errors") or []
    present = dashboard_present(args.host)
    print(f"KIBANA_DASHBOARD id={DASHBOARD_ID} imported={result.get('successCount', 0)} "
          f"errors={len(errors)} present={str(present).lower()} source={NDJSON.relative_to(C.PROJECT_ROOT)}")
    for e in errors:
        print(f"KIBANA_IMPORT_ERROR {json.dumps(e)}")
    sys.exit(0 if not errors and present else 1)


if __name__ == "__main__":
    main()
