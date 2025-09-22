import sys
import os
import json
import math
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Iterable
from urllib.parse import urlsplit


LOG_PATH = None  # Log path must be provided via CLI argument


def read_lines(path: str) -> Iterable[str]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield line


def parse_json_after_bracket(line: str) -> dict:
    # lines look like: [ts] {json}
    idx = line.find("{")
    if idx == -1:
        return {}
    try:
        return json.loads(line[idx:])
    except Exception:
        return {}


def p95(values: List[int]) -> float:
    if not values:
        return float("nan")
    arr = sorted(values)
    k = int(math.ceil(0.95 * len(arr))) - 1
    k = max(0, min(k, len(arr) - 1))
    return float(arr[k])


def human_stats(values: List[int]) -> Dict[str, float]:
    if not values:
        return {"count": 0, "avg_ms": float("nan"), "median_ms": float("nan"), "p95_ms": float("nan"), "min_ms": float("nan"), "max_ms": float("nan")}
    arr = sorted(values)
    mid = len(arr) // 2
    if len(arr) % 2 == 1:
        median = float(arr[mid])
    else:
        median = (arr[mid - 1] + arr[mid]) / 2.0
    return {
        "count": len(values),
        "avg_ms": sum(values) / len(values),
        "median_ms": median,
        "p95_ms": p95(values),
        "min_ms": min(values),
        "max_ms": max(values),
    }


def bucket_histogram(values: List[int]) -> Dict[str, int]:
    # Log-spaced-ish buckets in ms
    buckets = [10, 20, 50, 100, 200, 300, 500, 800, 1000, 1500, 2000, 3000, 5000, 8000]
    hist: Dict[str, int] = Counter()
    for v in values:
        placed = False
        for b in buckets:
            if v <= b:
                hist[f"<= {b}ms"] += 1
                placed = True
                break
        if not placed:
            hist["> 8000ms"] += 1
    return dict(hist)


def base_url_of(url: str) -> str:
    parts = urlsplit(url)
    path = parts.path or "/"
    if "/" in path:
        base_path = path.rsplit("/", 1)[0] + "/"
    else:
        base_path = "/"
    return f"{parts.scheme}://{parts.netloc}{base_path}"


def analyze(path: str) -> dict:
    per_gateway_times: Dict[str, List[int]] = defaultdict(list)
    per_cid_records: Dict[str, List[Tuple[str, int]]] = defaultdict(list)  # cid -> [(gateway, elapsed_ms)]
    per_gateway_total: Counter[str] = Counter()

    for line in read_lines(path):
        if '"event": "gateway_fetch"' not in line:
            continue
        obj = parse_json_after_bracket(line)
        if not obj:
            continue
        cid = obj.get("cid")
        url = obj.get("url")
        elapsed = obj.get("elapsed_ms")
        status = obj.get("status")
        if not cid or not url or not isinstance(elapsed, int):
            continue
        # derive gateway label as base url
        gateway = base_url_of(url)
        per_gateway_total[gateway] += 1
        # include only successful responses by default; count non-2xx separately
        if 200 <= int(status) < 300:
            per_gateway_times[gateway].append(elapsed)
            per_cid_records[cid].append((gateway, elapsed))

    # compute stats per gateway
    gateway_stats = {gw: human_stats(times) for gw, times in per_gateway_times.items()}

    # fastest wins per CID
    wins: Counter[str] = Counter()
    ties: int = 0
    for cid, items in per_cid_records.items():
        if not items:
            continue
        best_time = min(t for _, t in items)
        winners = [gw for gw, t in items if t == best_time]
        if len(winners) == 1:
            wins[winners[0]] += 1
        else:
            ties += 1

    # histograms per gateway
    gateway_hists = {gw: bucket_histogram(times) for gw, times in per_gateway_times.items()}

    # add comparative narrative hints
    narrative: List[str] = []
    if gateway_stats:
        sorted_by_avg = sorted(gateway_stats.items(), key=lambda kv: kv[1]["avg_ms"])
        best, worst = sorted_by_avg[0][0], sorted_by_avg[-1][0]
        narrative.append(f"Average latency: best={best}, worst={worst}.")
        sorted_by_p95 = sorted(gateway_stats.items(), key=lambda kv: kv[1]["p95_ms"])
        best_p95, worst_p95 = sorted_by_p95[0][0], sorted_by_p95[-1][0]
        narrative.append(f"P95 latency: best={best_p95}, worst={worst_p95}.")
    if wins:
        top_wins = wins.most_common(3)
        narrative.append("Fastest-win counts: " + ", ".join(f"{gw}={cnt}" for gw, cnt in top_wins))
    if ties:
        narrative.append(f"Tied fastest results: {ties}")

    return {
        "path": path,
        "gateways": list(per_gateway_times.keys()),
        "gateway_stats": gateway_stats,
        "gateway_samples": {gw: len(times) for gw, times in per_gateway_times.items()},
        "gateway_total_attempts": dict(per_gateway_total),
        "fastest_wins": dict(wins),
        "ties": ties,
        "histograms": gateway_hists,
        "notes": narrative,
    }


def format_report(result: dict) -> str:
    gateway_stats: Dict[str, dict] = result.get("gateway_stats", {})
    histograms: Dict[str, dict] = result.get("histograms", {})
    wins: Dict[str, int] = result.get("fastest_wins", {})
    totals: Dict[str, int] = result.get("gateway_total_attempts", {})

    # Order gateways by average latency
    order = sorted(gateway_stats.keys(), key=lambda gw: (math.inf if math.isnan(gateway_stats[gw]["avg_ms"]) else gateway_stats[gw]["avg_ms"]))

    lines: List[str] = []
    lines.append("网关性能报告\n")
    total_wins = sum(wins.values()) or 1
    for gw in order:
        stats = gateway_stats[gw]
        count = int(stats["count"]) if not math.isnan(stats["count"]) else 0
        avg = stats["avg_ms"]
        med = stats.get("median_ms", float("nan"))
        p95v = stats["p95_ms"]
        minv = stats["min_ms"]
        maxv = stats["max_ms"]
        gw_wins = wins.get(gw, 0)
        win_pct = (gw_wins * 100.0) / total_wins if total_wins else 0.0
        lines.append(f"网关: {gw}")
        lines.append(f"  请求数: {count}")
        lines.append(f"  平均耗时: {avg:.1f} ms")
        lines.append(f"  中位耗时: {med:.1f} ms")
        lines.append(f"  P95耗时: {p95v:.1f} ms")
        lines.append(f"  耗时范围: {int(minv) if not math.isnan(minv) else 'NaN'} - {int(maxv) if not math.isnan(maxv) else 'NaN'} ms")
        lines.append(f"  更快次数: {gw_wins} ({win_pct:.1f}%)")
        # Histogram
        hist = histograms.get(gw, {})
        if hist:
            lines.append("  耗时分布:")
            # stable order by bucket upper bound
            def bucket_key(k: str) -> Tuple[int, int]:
                if k.startswith("<= ") and k.endswith("ms"):
                    try:
                        return (0, int(k[3:-2]))
                    except Exception:
                        return (0, 10**9)
                if k.startswith("> ") and k.endswith("ms"):
                    try:
                        return (1, int(k[2:-2]))
                    except Exception:
                        return (1, 10**9)
                return (2, 10**9)
            for bucket in sorted(hist.keys(), key=bucket_key):
                lines.append(f"    {bucket}: {hist[bucket]}")
        lines.append("")

    # Notes
    notes = result.get("notes", [])
    if notes:
        lines.append("结论与说明:")
        for n in notes:
            lines.append(f"- {n}")

    return "\n".join(lines)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python analyze_gateway_logs.py [/path/to/pump.log] [--json]", file=sys.stderr)
        return 2
    args = sys.argv[1:]
    to_json = False
    if args and args[-1] == "--json":
        to_json = True
        args = args[:-1]
    if not args:
        print("Usage: python analyze_gateway_logs.py [/path/to/pump.log] [--json]", file=sys.stderr)
        return 2
    path = args[0]
    result = analyze(path)
    if to_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(format_report(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


