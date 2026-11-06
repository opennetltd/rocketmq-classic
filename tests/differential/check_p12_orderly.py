#!/usr/bin/env python3
"""Bounded file-gate assertions for the P12 live orderly-consumer handoff."""

from __future__ import annotations

import sys
import time
from pathlib import Path


def marker(directory: Path, language: str, broker: str, body: str, offset: int = 0) -> Path:
    return directory / f"{language}-{broker}-0-{offset}-{body}.seen"


def wait_for(predicate, timeout: float, message: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise SystemExit(message)


def held(directory: Path, ownership_path: Path, topic: str, timeout: float) -> None:
    names = [
        marker(directory, "java", "broker-a", "p12-slow-a"),
        marker(directory, "java", "broker-b", "p12-slow-b"),
    ]
    wait_for(
        lambda: (
            all(path.is_file() and path.read_text().strip() == "begin" for path in names)
            and ownership_path.is_file()
            and all(
                f"{topic}|{broker}|0|true" in ownership_path.read_text().splitlines()
                for broker in ("broker-a", "broker-b")
            )
        ),
        timeout,
        "Java did not hold both slow callbacks with confirmed Broker locks",
    )
    print("P12\tHELD\tjava	broker-a:0,broker-b:0")


def handoff(directory: Path, rust_ownership: Path, topic: str, timeout: float) -> None:
    def transferred() -> str | None:
        for broker, suffix in (("broker-a", "a"), ("broker-b", "b")):
            java_slow = marker(directory, "java", broker, f"p12-slow-{suffix}")
            rust_slow = marker(directory, "rust", broker, f"p12-slow-{suffix}")
            rust_next = marker(directory, "rust", broker, f"p12-next-{suffix}", offset=1)
            if not all(path.is_file() for path in (java_slow, rust_slow, rust_next)):
                continue
            if java_slow.read_text().strip() != "begin":
                continue
            ownership = rust_ownership.read_text() if rust_ownership.exists() else ""
            expected = f"{topic}|{broker}|0|true"
            if expected in ownership.splitlines():
                return broker
        return None

    broker = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        broker = transferred()
        if broker:
            break
        time.sleep(0.1)
    if broker is None:
        raise SystemExit(
            "Rust did not acquire a locked queue and consume both redelivered offsets "
            "while Java's original slow callback remained in flight"
        )
    print(f"P12\tHANDOFF\t{broker}:0\tjava_callback_still_in_flight\trust_committed_offsets=0,1")


def java_ended(directory: Path, timeout: float) -> None:
    names = [
        marker(directory, "java", "broker-a", "p12-slow-a"),
        marker(directory, "java", "broker-b", "p12-slow-b"),
    ]
    wait_for(
        lambda: all(path.is_file() and path.read_text().strip() == "end" for path in names),
        timeout,
        "Java slow callbacks did not leave the listener gate",
    )
    print("P12\tCALLBACKS_RELEASED\tjava	broker-a:0,broker-b:0")


def java_still_held(directory: Path) -> None:
    names = [
        marker(directory, "java", "broker-a", "p12-slow-a"),
        marker(directory, "java", "broker-b", "p12-slow-b"),
    ]
    if not all(path.is_file() and path.read_text().strip() == "begin" for path in names):
        raise SystemExit("Java callback gate was released before the Rust consumer stopped")
    print("P12\tSTILL_HELD\tjava\tbroker-a:0,broker-b:0")


def offset(path: Path, expected: int) -> None:
    lines = path.read_text().splitlines()
    values = [line.split("\t", 1)[1] for line in lines if line.startswith("OFFSET\t")]
    if values != [str(expected)]:
        raise SystemExit(f"expected Broker offset {expected} in {path}, got {values!r}")
    print(f"P12\tOFFSET\t{path.name}\t{expected}")


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: check_p12_orderly.py held|handoff|java-ended|offset ...")
    command = sys.argv[1]
    if command == "held" and len(sys.argv) == 6:
        held(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], float(sys.argv[5]))
    elif command == "handoff" and len(sys.argv) == 6:
        handoff(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], float(sys.argv[5]))
    elif command == "java-ended" and len(sys.argv) == 4:
        java_ended(Path(sys.argv[2]), float(sys.argv[3]))
    elif command == "java-still-held" and len(sys.argv) == 3:
        java_still_held(Path(sys.argv[2]))
    elif command == "offset" and len(sys.argv) == 4:
        offset(Path(sys.argv[2]), int(sys.argv[3]))
    else:
        raise SystemExit("invalid P12 checker arguments")


if __name__ == "__main__":
    main()
