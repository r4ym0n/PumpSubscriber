import asyncio
import json
import time
import os
import re
from typing import Optional, Dict, Any, List
from datetime import datetime

import websockets
from pin_to_pinata import pin_once
from fetch_cid_time import download_discard, build_urls


NATS_WS_URL = "wss://prod-advanced.nats.realtime.pump.fun/"
RAW_LOG_PATH = "raw.log"
PARSED_LOG_PATH = "pump.log"


class NatsWsProtocolError(Exception):
    pass


def now_ts_ms() -> str:
    # Returns local time with millisecond precision
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


async def write_line(path: str, line: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        if not line.endswith("\n"):
            f.write("\n")


async def handle_msg(subject: str, payload: str) -> None:
    ts = now_ts_ms()
    await write_line(RAW_LOG_PATH, f"[{ts}] MSG {subject} {len(payload)}")
    await write_line(RAW_LOG_PATH, payload)
    try:
        text = payload
        if text.startswith('"') and text.endswith('"'):
            text = json.loads(text)
        obj = json.loads(text)
        if isinstance(obj, dict):
            mint = obj.get("mint")
            image = obj.get("image")
            await write_line(
                PARSED_LOG_PATH,
                f"[{ts}] " + json.dumps(
                    {"ts": ts, "subject": subject, "mint": mint, "image": image},
                    ensure_ascii=False,
                ),
            )
            # Try to extract CID(s) from the message and process them
            for cid in extract_cids(obj):
                asyncio.create_task(process_cid(cid, subject))
        else:
            await write_line(
                PARSED_LOG_PATH,
                f"[{ts}] " + json.dumps({"ts": ts, "subject": subject, "non_object": True}, ensure_ascii=False),
            )
    except Exception as exc:
        await write_line(
            PARSED_LOG_PATH,
            f"[{ts}] " + json.dumps(
                {"ts": ts, "subject": subject, "error": str(exc), "payload_preview": payload[:200]},
                ensure_ascii=False,
            ),
        )


def build_connect_options() -> Dict[str, Any]:
    # Allow env overrides, but default to provided values
    user = os.getenv("PUMP_NATS_USER", "subscriber")
    password = os.getenv("PUMP_NATS_PASS", os.getenv("PUMP_NATS_PASSWORD", "OktDhmZ2D3CtYUiM"))
    opts: Dict[str, Any] = {
        "no_responders": True,
        "protocol": 1,
        "verbose": False,
        "pedantic": False,
        "user": user,
        "pass": password,
        "lang": "nats.ws",
        "version": "1.30.3",
        "headers": True,
    }

    # If token/JWT provided, they take precedence over user/pass
    token = os.getenv("PUMP_NATS_TOKEN")
    if token:
        opts.pop("user", None)
        opts.pop("pass", None)
        opts["auth_token"] = token
    jwt = os.getenv("PUMP_NATS_JWT")
    sig = os.getenv("PUMP_NATS_SIG")
    if jwt:
        opts.pop("user", None)
        opts.pop("pass", None)
        opts["jwt"] = jwt
        if sig:
            opts["sig"] = sig

    return opts


_CID_PATTERNS: List[re.Pattern[str]] = [
    re.compile(r"ipfs://(?P<cid>[A-Za-z0-9]+)", re.IGNORECASE),
    re.compile(r"/ipfs/(?P<cid>[A-Za-z0-9]+)", re.IGNORECASE),
    re.compile(r"^(?P<cid>Qm[1-9A-HJ-NP-Za-km-z]{44})$"),  # CIDv0 approx
    re.compile(r"^(?P<cid>bafy[\w]{20,})$", re.IGNORECASE),  # CIDv1 approx
]


def extract_cids(obj: Dict[str, Any]) -> List[str]:
    cids: List[str] = []
    # Direct field
    direct = obj.get("cid")
    if isinstance(direct, str) and direct:
        cids.append(direct)
    # Image field may contain ipfs URL or CID
    image = obj.get("image")
    if isinstance(image, str) and image:
        for pat in _CID_PATTERNS:
            m = pat.search(image)
            if m:
                cids.append(m.group("cid"))
                break
        else:
            # Fallback: bare token-like
            token = image.strip()
            if token and (token.startswith("Qm") or token.lower().startswith("bafy")):
                cids.append(token)
    # Deduplicate preserving order
    seen = set()
    result: List[str] = []
    for c in cids:
        if c and c not in seen:
            seen.add(c)
            result.append(c)
    return result


async def process_cid(cid: str, subject: str) -> None:
    ts = now_ts_ms()
    # Run Pinata pin and first gateway fetch concurrently
    async def do_pin() -> None:
        jwt = os.getenv("PINATA_JWT") or os.getenv("PINATA_BEARER")
        if not jwt:
            await write_line(
                PARSED_LOG_PATH,
                f"[{now_ts_ms()}] " + json.dumps({
                    "ts": now_ts_ms(),
                    "event": "pinata_skip_no_jwt",
                    "cid": cid,
                    "subject": subject,
                }, ensure_ascii=False),
            )
            return
        timeout_s = float(os.getenv("PINATA_TIMEOUT_SECONDS", "15"))
        start = time.perf_counter()
        status, body = await asyncio.to_thread(pin_once, jwt, cid, timeout_s)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        await write_line(
            PARSED_LOG_PATH,
            f"[{now_ts_ms()}] " + json.dumps({
                "ts": now_ts_ms(),
                "event": "pinata_pin_by_cid",
                "cid": cid,
                "subject": subject,
                "status": status,
                "elapsed_ms": elapsed_ms,
                "ok": 200 <= status < 300,
                "response": body,
            }, ensure_ascii=False),
        )

    async def do_fetch() -> None:
        timeout_s = float(os.getenv("GATEWAY_TIMEOUT_SECONDS", "20"))
        urls = build_urls(cid)
        # Launch all configured gateways concurrently and log as each completes
        async def fetch_and_log(url: str) -> None:
            start = time.perf_counter()
            try:
                status, size_bytes, err = await asyncio.to_thread(download_discard, url, timeout_s)
            except Exception as e:
                status, size_bytes, err = 0, 0, str(e)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            await write_line(
                PARSED_LOG_PATH,
                f"[{now_ts_ms()}] " + json.dumps({
                    "ts": now_ts_ms(),
                    "event": "gateway_fetch",
                    "cid": cid,
                    "subject": subject,
                    "url": url,
                    "status": status,
                    "elapsed_ms": elapsed_ms,
                    "size_bytes": size_bytes,
                    "ok": 200 <= status < 300,
                    "error": err,
                }, ensure_ascii=False),
            )

        await asyncio.gather(*(fetch_and_log(u) for u in urls))

    await asyncio.gather(do_pin(), do_fetch())


async def nats_ws_subscribe(url: str) -> None:
    backoff_seconds = 1
    while True:
        try:
            headers = {
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
            swk = os.getenv("PUMP_WS_KEY", "IgTbIuAbBcB0BmbCyLtDKA==")
            headers["Sec-WebSocket-Key"] = swk
            bearer = os.getenv("PUMP_WS_BEARER")
            if bearer:
                headers["Authorization"] = f"Bearer {bearer}"

            async with websockets.connect(
                url,
                subprotocols=["nats"],
                ping_interval=None,
                max_size=20_000_000,
                extra_headers=headers if headers else None,
                origin="https://pump.fun",
                user_agent_header=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/140.0.0.0 Safari/537.36"
                ),
                compression="deflate",
            ) as ws:
                # Read until we get INFO
                partial = ""
                info_received = False
                while not info_received:
                    more = await ws.recv()
                    if isinstance(more, bytes):
                        more = more.decode("utf-8", errors="replace")
                    partial += more
                    while "\r\n" in partial:
                        line, partial = partial.split("\r\n", 1)
                        if not line:
                            continue
                        if line.startswith("INFO "):
                            info_received = True
                            try:
                                info_obj = json.loads(line[5:].strip())
                                await write_line(PARSED_LOG_PATH, json.dumps({"ts": now_ts_ms(), "event": "server_info", "info_keys": sorted(list(info_obj.keys()))}, ensure_ascii=False))
                            except Exception:
                                pass
                            break
                        elif line.startswith("PING"):
                            await ws.send("PONG\r\n")
                        elif line.startswith("-ERR"):
                            raise NatsWsProtocolError(line)

                # CONNECT with provided auth/options
                connect_opts = build_connect_options()
                await ws.send("CONNECT " + json.dumps(connect_opts) + "\r\n")

                # SUB both subjects with SIDs 4 and 2
                await ws.send("SUB advancedNewCoinCreated 4\r\n")
                await ws.send("SUB coinImageUpdated.> 2\r\n")
                await ws.send("SUB _WARMUP_ADVANCED_1758175505128 5\r\n")
                

                await ws.send("PING\r\n")

                expected_payload: Optional[int] = None
                expected_header_len: Optional[int] = None
                current_subject: Optional[str] = None

                while True:
                    if "\r\n" not in partial and expected_payload is None:
                        more = await ws.recv()
                        if isinstance(more, bytes):
                            more = more.decode("utf-8", errors="replace")
                        partial += more
                        continue

                    if expected_payload is not None:
                        if len(partial) < expected_payload + 2:
                            more = await ws.recv()
                            if isinstance(more, bytes):
                                more = more.decode("utf-8", errors="replace")
                            partial += more
                            continue

                        payload_block = partial[:expected_payload]
                        # Skip trailing CRLF after payload
                        if partial[expected_payload:expected_payload + 2] == "\r\n":
                            partial = partial[expected_payload + 2:]
                        else:
                            partial = partial[expected_payload:]

                        if expected_header_len is not None:
                            # HMSG: first expected_header_len bytes are headers, remainder is body
                            hdr = payload_block[:expected_header_len]
                            body = payload_block[expected_header_len:]
                            # Optionally parse hdr if needed; ignoring for now
                            await handle_msg(current_subject or "", body)
                        else:
                            await handle_msg(current_subject or "", payload_block)

                        expected_payload = None
                        expected_header_len = None
                        current_subject = None
                        continue

                    line, partial = partial.split("\r\n", 1)
                    if not line:
                        continue
                    if line.startswith("PING"):
                        await ws.send("PONG\r\n")
                        continue
                    if line.startswith("PONG"):
                        continue
                    if line.startswith("INFO "):
                        continue
                    if line.startswith("-ERR"):
                        raise NatsWsProtocolError(line)
                    if line.startswith("MSG "):
                        # MSG <subject> <sid> <size> [reply-to]
                        parts = line.split()
                        if len(parts) < 4:
                            raise NatsWsProtocolError(f"Bad MSG header: {line}")
                        current_subject = parts[1]
                        try:
                            expected_payload = int(parts[3])
                        except ValueError:
                            raise NatsWsProtocolError(f"Invalid length in MSG: {line}")
                        expected_header_len = None
                        continue
                    if line.startswith("HMSG "):
                        # HMSG <subject> <sid> <hdr_len> <total_len> [reply-to]
                        parts = line.split()
                        if len(parts) < 5:
                            raise NatsWsProtocolError(f"Bad HMSG header: {line}")
                        current_subject = parts[1]
                        try:
                            expected_header_len = int(parts[3])
                            expected_payload = int(parts[4])
                        except ValueError:
                            raise NatsWsProtocolError(f"Invalid lengths in HMSG: {line}")
                        continue
                    # Unknown line: ignore

                backoff_seconds = 1
        except Exception as exc:
            await write_line(PARSED_LOG_PATH, json.dumps({"ts": now_ts_ms(), "event": "reconnect", "error": str(exc), "backoff_s": backoff_seconds}, ensure_ascii=False))
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)


async def main() -> None:
    await nats_ws_subscribe(NATS_WS_URL)


if __name__ == "__main__":
    asyncio.run(main())
