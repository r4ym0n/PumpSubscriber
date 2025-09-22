import sys
import os
import json
import time
from typing import Iterable, Optional, Tuple
from urllib import request, error


PINATA_ENDPOINT = "https://api.pinata.cloud/v3/files/public/pin_by_cid"


def iter_cids_from_args_or_stdin(argv: list[str]) -> Iterable[str]:
    if len(argv) > 1:
        for cid in argv[1:]:
            cid = cid.strip()
            if cid:
                yield cid
        return

    # No args: read from stdin, one CID per line (ignore empty/comment lines)
    if sys.stdin.isatty():
        print("Usage: python pin_to_pinata.py <CID> [CID ...]  # or pipe CIDs via stdin", file=sys.stderr)
        return

    for line in sys.stdin:
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        # Allow JSON lines like {"cid": "..."}
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


def build_request(jwt: str, cid: str) -> request.Request:
    data = json.dumps({"cid": cid}).encode("utf-8")
    req = request.Request(PINATA_ENDPOINT, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {jwt}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    return req


def pin_once(jwt: str, cid: str, timeout_s: float) -> Tuple[int, Optional[dict]]:
    req = build_request(jwt, cid)
    try:
        with request.urlopen(req, timeout=timeout_s) as resp:
            status = resp.getcode()
            body_bytes = resp.read()
            try:
                body = json.loads(body_bytes.decode("utf-8", errors="replace")) if body_bytes else None
            except Exception:
                body = None
            return status, body
    except error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        print(f"pinata_http_error cid={cid} status={e.code} body={body[:500]}", file=sys.stderr)
        return e.code, None
    except Exception as e:
        print(f"pinata_error cid={cid} error={e}", file=sys.stderr)
        return 0, None


def main() -> int:
    jwt = os.getenv("PINATA_JWT") or os.getenv("PINATA_BEARER")
    # Allow passing JWT as first arg flag
    argv = sys.argv
    if len(argv) >= 3 and argv[1] in ("--jwt", "-j"):
        jwt = argv[2]
        argv = [argv[0]] + argv[3:]

    if not jwt:
        print("Missing PINATA_JWT. Set env PINATA_JWT or use --jwt <token>.", file=sys.stderr)
        return 2

    timeout_s = float(os.getenv("PINATA_TIMEOUT_SECONDS", "15"))

    had_error = False
    for cid in iter_cids_from_args_or_stdin(argv):
        start = time.perf_counter()
        status, body = pin_once(jwt, cid, timeout_s)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        ok = 200 <= status < 300
        had_error |= not ok
        # Print a compact, parseable line
        out = {
            "action": "pinata_pin_by_cid",
            "cid": cid,
            "status": status,
            "elapsed_ms": elapsed_ms,
            "ok": ok,
        }
        if body is not None:
            out["response"] = body
        print(json.dumps(out, ensure_ascii=False))

    return 1 if had_error else 0


if __name__ == "__main__":
    raise SystemExit(main())


