#!/usr/bin/env python3
"""Compare P17 live query outputs and verify injected partial/terminal failures."""

import sys
from pathlib import Path

NO_MESSAGE_RESPONSE_CODE = 208


def rows(directory, name):
    return [line.split("\t") for line in (directory / name).read_text().splitlines() if line]


def result_count(directory, name, key):
    matches = [row for row in rows(directory, name) if row[:3] == ["P17", "RESULT", key]]
    if len(matches) != 1:
        raise SystemExit(f"{name}: expected one result summary for {key}, found {len(matches)}")
    return int(matches[0][3])


def messages(directory, name, key):
    selected = []
    for row in rows(directory, name):
        if row[:3] != ["P17", "MESSAGE", key]:
            continue
        if len(row) != 7:
            raise SystemExit(f"{name}: malformed message row for {key}: {row}")
        broker_name = row[3] if row[3] not in ("", "null") else "<not-provided>"
        selected.append((broker_name, int(row[4]), int(row[5]), row[6]))
    return sorted(selected)


def compare_pair(directory, java_file, rust_file, key, expected_count):
    if result_count(directory, java_file, key) != expected_count:
        raise SystemExit(f"{java_file}: unexpected {key} result count")
    if result_count(directory, rust_file, key) != expected_count:
        raise SystemExit(f"{rust_file}: unexpected {key} result count")
    java_messages = messages(directory, java_file, key)
    rust_messages = messages(directory, rust_file, key)
    if java_messages != rust_messages:
        raise SystemExit(
            f"Java/Rust {key} query results differ:\nJava={java_messages}\nRust={rust_messages}"
        )
    return java_messages


def check_prepare(directory):
    duplicate = compare_pair(
        directory, "java-duplicate.tsv", "rust-duplicate.tsv", "p17-duplicate-key", 2
    )
    if [row[3] for row in duplicate] != [
        "p17-duplicate-broker-a", "p17-duplicate-broker-b"
    ]:
        raise SystemExit(f"duplicate-key messages did not match the two-Broker seed: {duplicate}")
    bounded = compare_pair(
        directory, "java-bounded-all.tsv", "rust-bounded-all.tsv", "p17-bounded-time-range", 2
    )
    by_body = {row[3]: row for row in bounded}
    if set(by_body) != {"p17-bounded-in-range", "p17-bounded-out-of-range"}:
        raise SystemExit(f"bounded-time fixture messages differ: {bounded}")
    in_timestamp = by_body["p17-bounded-in-range"][2]
    out_timestamp = by_body["p17-bounded-out-of-range"][2]
    begin_timestamp = in_timestamp - 500
    end_timestamp = in_timestamp + 1_000
    if out_timestamp <= end_timestamp or out_timestamp - in_timestamp < 1_500:
        raise SystemExit(
            f"bounded-time messages were too close for a strict range: in={in_timestamp}, out={out_timestamp}"
        )
    (directory / "bounded-range.txt").write_text(
        f"begin_timestamp={begin_timestamp}\nend_timestamp={end_timestamp}\n"
    )
    print(f"P17_PREPARED\t{begin_timestamp}\t{end_timestamp}")


def check_verify(directory):
    check_prepare(directory)
    narrow = compare_pair(
        directory, "java-bounded-range.tsv", "rust-bounded-range.tsv",
        "p17-bounded-time-range", 1
    )
    if narrow[0][3] != "p17-bounded-in-range":
        raise SystemExit(f"bounded range returned the wrong message: {narrow}")
    partial = compare_pair(
        directory, "java-partial.tsv", "rust-partial.tsv", "p17-duplicate-key", 1
    )
    if (
        len(partial) != 1
        or partial[0][1] != 0
        or partial[0][3] != "p17-duplicate-broker-a"
    ):
        raise SystemExit(f"partial Broker failure did not preserve the surviving Broker result: {partial}")
    for name in ("java-terminal.tsv", "rust-terminal.tsv"):
        errors = [
            row for row in rows(directory, name)
            if row[:3] == ["P17", "ERROR", "p17-duplicate-key"]
        ]
        if len(errors) != 1 or errors[0] != [
            "P17", "ERROR", "p17-duplicate-key", str(NO_MESSAGE_RESPONSE_CODE)
        ]:
            raise SystemExit(
                f"{name}: terminal errors did not map to NO_MESSAGE "
                f"({NO_MESSAGE_RESPONSE_CODE}): {errors}"
            )
    for name, label, minimum in (
        ("proxy-a-events.tsv", "broker-a", 2),
        ("proxy-b-events.tsv", "broker-b", 4),
    ):
        count = sum(
            1 for row in rows(directory, name)
            if row[:3] == ["P17_PROXY", label, "INJECTED_QUERY_ERROR"]
        )
        if count < minimum:
            raise SystemExit(f"{name}: expected at least {minimum} injected query errors, saw {count}")
    (directory / "result.txt").write_text(
        "PASS P17-V02: Java and Rust returned the same duplicate-key messages from both Brokers, "
        "the timestamp-bounded query returned only the in-range message, one Broker query failure "
        "preserved the other Broker's result, and terminal failures mapped to NO_MESSAGE\n"
    )
    print((directory / "result.txt").read_text(), end="")


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("prepare", "verify"):
        raise SystemExit("usage: check_p17_admin_query.py prepare|verify <evidence-directory>")
    directory = Path(sys.argv[2])
    if sys.argv[1] == "prepare":
        check_prepare(directory)
    else:
        check_verify(directory)


if __name__ == "__main__":
    main()
