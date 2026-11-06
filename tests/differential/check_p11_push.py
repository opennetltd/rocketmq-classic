#!/usr/bin/env python3
"""Checks archived P11-V04 bidirectional Push and lifecycle evidence."""

from __future__ import annotations

import sys
from pathlib import Path


def rows(root: Path, name: str, prefix: tuple[str, ...]) -> list[list[str]]:
    path = root / name
    if not path.is_file():
        raise SystemExit(f"missing P11-V04 evidence file: {name}")
    found = []
    for line in path.read_text().splitlines():
        fields = line.split("\t")
        if fields[: len(prefix)] == list(prefix):
            found.append(fields)
    return found


def one_row(root: Path, name: str, prefix: tuple[str, ...], expected_len: int) -> list[str]:
    found = rows(root, name, prefix)
    if len(found) != 1 or len(found[0]) != expected_len:
        raise SystemExit(f"{name}: expected one {prefix!r} row, found {found!r}")
    return found[0]


def check_offset(root: Path, suffix: str) -> int:
    java = one_row(root, f"java-offset-{suffix}.tsv", ("OFFSET",), 2)
    rust = one_row(root, f"rust-offset-{suffix}.tsv", ("OFFSET",), 2)
    java_offset = int(java[1])
    rust_offset = int(rust[1])
    if java_offset < 1 or rust_offset < 1 or java_offset != rust_offset:
        raise SystemExit(
            f"Broker {suffix} consumer offsets differ or did not advance: "
            f"Java={java_offset} Rust={rust_offset}"
        )
    return java_offset


def main() -> int:
    if len(sys.argv) != 5:
        raise SystemExit("usage: check_p11_push.py <evidence-dir> <topic> <java-body> <rust-body>")
    root = Path(sys.argv[1])
    topic, java_body, rust_body = sys.argv[2:]

    java_produced = one_row(root, "java-producer.tsv", ("P11", "PRODUCED", "java"), 7)
    if java_produced[3:] != [java_body, "broker-a", "0", "0"]:
        raise SystemExit(f"Java producer did not seed the reviewed Broker A queue: {java_produced!r}")
    rust_produced = one_row(root, "rust-producer.tsv", ("P11", "PRODUCED", "rust"), 8)
    if rust_produced[3:] != [rust_body, "SEND_OK", "broker-b", "0", "0"]:
        raise SystemExit(f"Rust producer did not send to the reviewed Broker B queue: {rust_produced!r}")

    rust_deliveries = rows(root, "rust-push.tsv", ("P11", "DELIVERY", "rust"))
    rust_attempts = [row for row in rust_deliveries if len(row) == 9 and row[3] == java_body]
    if [int(row[4]) for row in rust_attempts] != [0, 1]:
        raise SystemExit(f"Rust Push did not observe the Java message with retry counts [0, 1]: {rust_attempts!r}")
    if any(row[5] not in (topic, f"%RETRY%compat_p11_push") for row in rust_attempts):
        raise SystemExit(f"Rust Push delivery topic differed from the scenario/retry topic: {rust_attempts!r}")

    java_deliveries = rows(root, "java-push.tsv", ("P11", "DELIVERY", "java"))
    java_attempts = [row for row in java_deliveries if len(row) == 8 and row[3] == rust_body]
    if [int(row[4]) for row in java_attempts] != [0, 1]:
        raise SystemExit(f"Java Push did not observe the Rust message with retry counts [0, 1]: {java_attempts!r}")
    if any(row[5] not in (topic, "%RETRY%compat_p11_push") for row in java_attempts):
        raise SystemExit(f"Java Push delivery topic differed from the scenario/retry topic: {java_attempts!r}")

    rust_cycles = rows(root, "rust-push.tsv", ("P11", "CYCLE", "rust"))
    if len(rust_cycles) != 3 or [row[3] for row in rust_cycles] != ["0", "1", "2"]:
        raise SystemExit(f"Rust Push did not complete three start/shutdown cycles: {rust_cycles!r}")
    if any(row[4:8] != ["0", "0", "0", "true"] for row in rust_cycles):
        raise SystemExit(f"Rust Push shutdown retained runtime work: {rust_cycles!r}")
    java_cycles = rows(root, "java-push.tsv", ("P11", "CYCLE", "java"))
    if len(java_cycles) != 3 or [row[3] for row in java_cycles] != ["0", "1", "2"]:
        raise SystemExit(f"Java Push did not complete three start/shutdown cycles: {java_cycles!r}")
    if any(row[4:6] != ["true", "true"] for row in java_cycles):
        raise SystemExit(f"Java Push shutdown retained a scheduled executor: {java_cycles!r}")

    rust_offsets = check_offset(root, "broker-a")
    java_offsets = check_offset(root, "broker-b")
    if rows(root, "rust-push.tsv", ("P11", "PASS", "RUST_PUSH_RETRY")) != [
        ["P11", "PASS", "RUST_PUSH_RETRY", "CYCLES=3", "RETRIES=1"]
    ]:
        raise SystemExit("Rust Push completion marker is missing")
    if rows(root, "java-push.tsv", ("P11", "PASS", "JAVA_PUSH_RETRY")) != [
        ["P11", "PASS", "JAVA_PUSH_RETRY", "CYCLES=3", "RETRIES=1"]
    ]:
        raise SystemExit("Java Push completion marker is missing")

    print(
        "P11-V04 evidence PASS: bidirectional Java/Rust Push delivery retried once, "
        f"Broker A/B offsets advanced equally ({rust_offsets}/{java_offsets}), and three shutdown cycles per client quiesced."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
