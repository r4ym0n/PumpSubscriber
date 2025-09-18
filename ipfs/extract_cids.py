#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
from typing import Iterable, Set


CID_PATTERN = re.compile(r"\b(Qm[1-9A-HJ-NP-Za-km-z]{44,}|b[a-z2-7]{58,}|B[A-Z2-7]{58,}|z[1-9A-HJ-NP-Za-km-z]{48,})\b")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def iter_lines(filepath: str) -> Iterable[str]:
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield line


def extract_cids_from_lines(lines: Iterable[str]) -> Set[str]:
    unique: Set[str] = set()
    for line in lines:
        for match in CID_PATTERN.findall(line):
            unique.add(match)
    return unique


def write_csv(output_csv: str, cids: Iterable[str]) -> None:
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cid"])
        for cid in sorted(cids):
            writer.writerow([cid])


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract CIDs from a log file into CSV")
    parser.add_argument("input", help="Path to cid.log or any text file containing CIDs")
    parser.add_argument("--output", "-o", default="cids.csv", help="Output CSV path (default: cids.csv)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    configure_logging(args.verbose)

    input_path = os.path.abspath(args.input)
    output_path = os.path.abspath(args.output)

    logging.info("Reading: %s", input_path)
    lines = iter_lines(input_path)
    cids = extract_cids_from_lines(lines)
    logging.info("Found %d unique CIDs", len(cids))

    logging.info("Writing CSV: %s", output_path)
    write_csv(output_path, cids)
    logging.info("Done")


if __name__ == "__main__":
    main()


