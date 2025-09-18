#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import time
from typing import Iterable, List, Dict, Any


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")


def run_ipfs_api(endpoint: str, arg: str) -> Dict[str, Any]:
    # Uses curl to call IPFS HTTP API to avoid requiring python lib
    url = f"http://127.0.0.1:5001/api/v0/{endpoint}?arg={arg}"
    proc = subprocess.run([
        "curl", "-sS", "--max-time", "30", url
    ], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"curl failed: {proc.returncode}, stderr={proc.stderr.strip()}")
    text = proc.stdout.strip()
    # Some endpoints stream ndjson; split lines and parse last stateful object and merge
    objects: List[Dict[str, Any]] = []
    for line in text.splitlines():
        if not line:
            continue
        try:
            objects.append(json.loads(line))
        except json.JSONDecodeError:
            logging.debug("Non-JSON line: %s", line)
            continue
    return {"_raw": text, "objects": objects}


def dht_findprovs(cid: str) -> List[Dict[str, Any]]:
    # IPFS HTTP API: /api/v0/dht/findprovs?arg=<cid>
    result = run_ipfs_api("dht/findprovs", cid)
    providers: List[Dict[str, Any]] = []
    for obj in result["objects"]:
        # Expect records like { "Type": <int>, "Responses": [{"ID": "peer", "Addrs": ["/ip4/.../p2p/<peer>"]}], ... }
        if "Responses" in obj and isinstance(obj["Responses"], list):
            for resp in obj["Responses"]:
                if not isinstance(resp, dict):
                    continue
                peer_id = resp.get("ID")
                addrs = resp.get("Addrs") or []
                if peer_id:
                    providers.append({"peer_id": peer_id, "addrs": addrs})
    return providers


def swarm_connect(addr: str, timeout_sec: int = 20) -> bool:
    # IPFS CLI is convenient for connect and has built-in retries
    proc = subprocess.run([
        "ipfs", "swarm", "connect", addr
    ], capture_output=True, text=True, timeout=timeout_sec)
    success = proc.returncode == 0
    if success:
        logging.info("Connected: %s | %s", addr, proc.stdout.strip())
    else:
        logging.warning("Connect failed: %s | %s %s", addr, proc.stdout.strip(), proc.stderr.strip())
    return success


def read_cids_from_csv(csv_path: str) -> List[str]:
    cids: List[str] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if not row:
                continue
            cids.append(row[0].strip())
    return cids


def main() -> None:
    parser = argparse.ArgumentParser(description="Find providers via IPFS API and connect to top X")
    parser.add_argument("--csv", default="cids.csv", help="CSV file with 'cid' header (default: cids.csv)")
    parser.add_argument("--top", type=int, default=5, help="Top X providers to connect to per CID")
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between connects")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    configure_logging(args.verbose)

    csv_path = os.path.abspath(args.csv)
    if not os.path.exists(csv_path):
        logging.error("CSV not found: %s", csv_path)
        sys.exit(2)

    cids = read_cids_from_csv(csv_path)
    logging.info("Loaded %d CIDs from %s", len(cids), csv_path)

    total_attempts = 0
    total_success = 0

    for cid in cids:
        logging.info("Finding providers for CID: %s", cid)
        try:
            providers = dht_findprovs(cid)
        except Exception as exc:
            logging.error("findprovs failed for %s: %s", cid, exc)
            continue

        logging.info("Found %d provider entries for CID %s", len(providers), cid)

        # Deduplicate providers by peer id
        dedup: Dict[str, Dict[str, Any]] = {}
        for p in providers:
            dedup[p["peer_id"]] = p
        top_providers = list(dedup.values())[: max(args.top, 0)]
        logging.info("Connecting to top %d providers for %s", len(top_providers), cid)

        for p in top_providers:
            peer_id = p["peer_id"]
            addrs: List[str] = p.get("addrs") or []
            if not addrs:
                logging.debug("No addrs for %s", peer_id)
                continue
            for addr in addrs:
                # Ensure multiaddr contains peer id suffix; if not, append /p2p/peer
                if f"/p2p/{peer_id}" not in addr and f"/ipfs/{peer_id}" not in addr:
                    candidate = addr.rstrip("/") + f"/p2p/{peer_id}"
                else:
                    candidate = addr
                total_attempts += 1
                ok = False
                try:
                    ok = swarm_connect(candidate)
                except subprocess.TimeoutExpired:
                    logging.warning("Connect timeout: %s", candidate)
                except Exception as exc:
                    logging.warning("Connect error %s: %s", candidate, exc)
                if ok:
                    total_success += 1
                    break
            if args.sleep > 0:
                time.sleep(args.sleep)

    logging.info("Done. Attempts=%d Success=%d", total_attempts, total_success)


if __name__ == "__main__":
    main()


