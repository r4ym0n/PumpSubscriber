import sys
import os
import time
import json
from typing import Iterable, Tuple, Optional
from urllib import request, error


DEFAULT_GATEWAYS = [
    "https://ipfs.io/ipfs/{cid}",
    "https://ok-test.mypinata.cloud/files/{cid}"
]


def iter_cids(argv: list[str]) -> Iterable[str]:
    if len(argv) > 1:
        for cid in argv[1:]:
            cid = cid.strip()
            if cid:
                yield cid
        return
    if sys.stdin.isatty():
        print("Usage: python fetch_cid_time.py <CID> [CID ...]  # or pipe CIDs via stdin", file=sys.stderr)
        return
    for line in sys.stdin:
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if text.startswith("{") and '"cid"' in text:
            try:
                obj = json.loads(text)
                cid = obj.get("cid")
                if isinstance(cid, str) and cid:
                    yield cid
                    continue
            except Exception:
                pass
        yield text


def build_urls(cid: str) -> list[str]:
    env_gateways = os.getenv("IPFS_GATEWAYS")
    if env_gateways:
        return [g.strip().format(cid=cid) for g in env_gateways.split(",") if g.strip()]
    return [tpl.format(cid=cid) for tpl in DEFAULT_GATEWAYS]


def download_discard(url: str, timeout_s: float) -> Tuple[int, int, Optional[str]]:
    req = request.Request(url, method="GET")
    req.add_header("Accept", "image/*,application/octet-stream;q=0.9,*/*;q=0.8")
    req.add_header("Cache-Control", "no-cache")
    req.add_header("Pragma", "no-cache")
    req.add_header("User-Agent", "cid-fetch-timer/1.0")
    try:
        with request.urlopen(req, timeout=timeout_s) as resp:
            status = resp.getcode()
            total_bytes = 0
            chunk = resp.read(64 * 1024)
            while chunk:
                total_bytes += len(chunk)
                chunk = resp.read(64 * 1024)
            return status, total_bytes, None
    except error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return e.code, 0, body[:300]
    except Exception as e:
        return 0, 0, str(e)


def main() -> int:
    argv = sys.argv
    timeout_s = float(os.getenv("GATEWAY_TIMEOUT_SECONDS", "20"))
    for cid in iter_cids(argv):
        urls = build_urls(cid)
        for url in urls:
            start = time.perf_counter()
            status, size_bytes, err = download_discard(url, timeout_s)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            ok = 200 <= status < 300
            out = {
                "action": "gateway_fetch",
                "cid": cid,
                "url": url,
                "status": status,
                "elapsed_ms": elapsed_ms,
                "size_bytes": size_bytes,
                "ok": ok,
            }
            if err:
                out["error"] = err
            print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


