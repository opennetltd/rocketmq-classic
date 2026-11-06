#!/usr/bin/env python3
"""Wait for a stable live assignment in the P11 mixed Java/Rust group test."""

from __future__ import annotations

import sys
import time
from pathlib import Path


def read_queues(path: Path) -> set[str]:
    try:
        return {line for line in path.read_text().splitlines() if line}
    except FileNotFoundError:
        return set()


def main() -> int:
    if len(sys.argv) != 7:
        raise SystemExit(
            "usage: check_mixed_ownership.py java-only|joined|rust-only "
            "<topic> <java-state> <rust-state> <java-exited-file> <timeout-seconds>"
        )
    mode, topic = sys.argv[1:3]
    java_path, rust_path, java_exited = map(Path, sys.argv[3:6])
    timeout_seconds = float(sys.argv[6])
    expected = {
        f"{topic}|{broker}|{queue_id}"
        for broker in ("broker-a", "broker-b")
        for queue_id in range(4)
    }
    if mode not in {"java-only", "joined", "rust-only"}:
        raise SystemExit(f"unknown convergence mode: {mode}")

    deadline = time.monotonic() + timeout_seconds
    previous: tuple[frozenset[str], frozenset[str]] | None = None
    stable_samples = 0
    maximum_overlap = 0
    transient_overlap_samples = 0
    latest_java: set[str] = set()
    latest_rust: set[str] = set()
    while time.monotonic() < deadline:
        latest_java = read_queues(java_path)
        latest_rust = read_queues(rust_path)
        overlap = latest_java & latest_rust
        maximum_overlap = max(maximum_overlap, len(overlap))
        if mode == "joined" and overlap:
            transient_overlap_samples += 1

        if mode == "java-only":
            valid = latest_java == expected and not latest_rust
        elif mode == "joined":
            valid = (
                bool(latest_java)
                and bool(latest_rust)
                and latest_java | latest_rust == expected
                and not overlap
            )
        else:
            valid = (
                java_exited.exists()
                and not latest_java
                and latest_rust == expected
            )

        if valid:
            current = (frozenset(latest_java), frozenset(latest_rust))
            stable_samples = stable_samples + 1 if current == previous else 1
            previous = current
            if stable_samples >= 4:
                print(
                    f"P11\tCONVERGENCE\t{mode}\tPASS\tstable_samples={stable_samples}"
                    f"\tjava_queues={len(latest_java)}\trust_queues={len(latest_rust)}"
                    f"\tmaximum_transient_overlap={maximum_overlap}"
                    f"\ttransient_overlap_samples={transient_overlap_samples}"
                )
                if mode == "joined":
                    print("P11\tTRANSIENT_REDELIVERY_POLICY\tALLOWED_DURING_CONVERGENCE")
                return 0
        else:
            stable_samples = 0
            previous = None
        time.sleep(0.5)

    print(
        f"P11\tCONVERGENCE\t{mode}\tTIMEOUT\tjava={sorted(latest_java)}"
        f"\trust={sorted(latest_rust)}\tjava_exited={java_exited.exists()}"
        f"\tmaximum_transient_overlap={maximum_overlap}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
