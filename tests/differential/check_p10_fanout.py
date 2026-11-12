#!/usr/bin/env python3
"""Validate the P10-V05 Push Pull fan-out evidence produced by both probes.

Usage:
    check_p10_fanout.py <evidence-dir> <topic-base> [option ...]
    check_p10_fanout.py --self-test

Each language probe writes `<language>-p10-fanout.tsv` in the evidence
directory with these tab-separated rows. Every timestamp is a monotonic
nanosecond value taken from that probe process only; clocks are never
compared across processes.

    P10F	ROUTE	<lang>	<topic>	<broker_name>	<queue_id>
    P10F	PULL_START	<lang>	<ts_ns>	<queue_id>	<opaque>	<suspend_ms>	<remote_addr>
    P10F	PULL_END	<lang>	<ts_ns>	<queue_id>	<opaque>	<response_code>
    P10F	GATE	<lang>	<ts_ns>	<span_ms>	<distinct_queues>
    P10F	SEND_START	<lang>	<ts_ns>	<queue_id>	<message_key>
    P10F	SEND_END	<lang>	<ts_ns>	<queue_id>	<message_key>	<status>	<offset>
    P10F	DELIVERY	<lang>	<ts_ns>	<queue_id>	<message_key>	<reconsume_times>
    P10F	SHUTDOWN	<lang>	<ts_ns>
"""

import sys
import tempfile
from pathlib import Path

TAG = "P10F"
LANGUAGES = ("java", "rust")
DEFAULT_QUEUES = 16
DEFAULT_ADMISSION_WINDOW_MS = 5000
DEFAULT_DELIVERY_WINDOW_MS = 5000
EXPECTED_SUSPEND_MILLIS = 15000


class EvidenceError(Exception):
    pass


def parse_rows(directory, language):
    path = Path(directory) / f"{language}-p10-fanout.tsv"
    if not path.is_file():
        raise EvidenceError(f"{language}: missing evidence file {path.name}")
    rows = {"ROUTE": [], "PULL_START": [], "PULL_END": [], "GATE": [],
            "SEND_START": [], "SEND_END": [], "DELIVERY": [], "SHUTDOWN": []}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) < 3 or fields[0] != TAG:
            raise EvidenceError(f"{language}:{number}: unrelated or malformed row: {line!r}")
        kind = fields[1]
        if kind not in rows:
            raise EvidenceError(f"{language}:{number}: unknown evidence kind {kind!r}")
        if fields[2] != language:
            raise EvidenceError(f"{language}:{number}: language column differs from the file")
        rows[kind].append(fields)
    return rows


def integer(value, label, language, number):
    try:
        return int(value)
    except ValueError as error:
        raise EvidenceError(f"{language}:{label} must be an integer ({value!r}): {error}") from error


def check_language(language, rows, topic_base, queues, admission_window_ms, delivery_window_ms):
    """Return (failures, per-queue elapsed ms, queues in flight at shutdown)."""
    failures = []

    def fail(message):
        failures.append(f"{language}: {message}")

    # 1. Route/assignment inventory.
    route_queues = {}
    for row in rows["ROUTE"]:
        if len(row) != 6:
            fail(f"malformed ROUTE row: {row!r}")
            continue
        topic, broker, queue_id = row[3], row[4], integer(row[5], "route queue id", language, 0)
        if topic != f"{topic_base}_{language}":
            fail(f"ROUTE row targets unexpected topic {topic!r}")
        if not broker:
            fail("ROUTE row has an empty broker name")
        if queue_id in route_queues:
            fail(f"duplicate ROUTE row for queue {queue_id}")
        route_queues[queue_id] = broker
    expected_queues = set(range(queues))
    if set(route_queues) != expected_queues:
        missing = sorted(expected_queues - set(route_queues))
        extra = sorted(set(route_queues) - expected_queues)
        fail(f"route queue inventory must be 0-{queues - 1}; missing={missing} unexpected={extra}")

    # 2. Pull request/response records and the requested suspend time.
    starts, ends = [], []
    for row in rows["PULL_START"]:
        if len(row) != 8:
            fail(f"malformed PULL_START row: {row!r}")
            continue
        stamp = integer(row[3], "pull start timestamp", language, 0)
        queue_id = integer(row[4], "pull start queue id", language, 0)
        opaque = integer(row[5], "pull start opaque", language, 0)
        suspend = integer(row[6], "pull start suspend time", language, 0)
        if not row[7]:
            fail("PULL_START row has an empty remote address")
        if suspend != EXPECTED_SUSPEND_MILLIS:
            fail(f"queue {queue_id} requested suspendTimeoutMillis={suspend}, "
                 f"expected {EXPECTED_SUSPEND_MILLIS}")
        starts.append((stamp, queue_id, opaque))
    for row in rows["PULL_END"]:
        if len(row) != 7:
            fail(f"malformed PULL_END row: {row!r}")
            continue
        stamp = integer(row[3], "pull end timestamp", language, 0)
        queue_id = integer(row[4], "pull end queue id", language, 0)
        opaque = integer(row[5], "pull end opaque", language, 0)
        ends.append((stamp, queue_id, opaque))

    start_opaques = [start[2] for start in starts]
    if len(set(start_opaques)) != len(start_opaques):
        fail("two Pull admissions reused one request identity (opaque)")
    end_opaques = [end[2] for end in ends]
    if len(set(end_opaques)) != len(end_opaques):
        fail("two Pull responses shared one request identity (opaque)")
    orphan_ends = sorted(set(end_opaques) - set(start_opaques))
    if orphan_ends:
        fail(f"Pull response(s) without a recorded admission: {orphan_ends}")

    start_by_queue = {}
    for stamp, queue_id, opaque in starts:
        start_by_queue.setdefault(queue_id, []).append((stamp, opaque))
    end_by_queue = {}
    for stamp, queue_id, opaque in ends:
        end_by_queue.setdefault(queue_id, []).append((stamp, opaque))

    # 3. Full target-topic assignment: every route queue issued its own Pull.
    if set(start_by_queue) != expected_queues:
        missing = sorted(expected_queues - set(start_by_queue))
        fail(f"queues never started a target Pull: {missing}")

    # 4. Admission barrier: all queues active within the window and before any response.
    gate = rows["GATE"]
    if len(gate) != 1:
        fail(f"exactly one GATE row is required, found {len(gate)}")
    elif len(gate[0]) != 6:
        fail(f"malformed GATE row: {gate[0]!r}")
    else:
        gate_span = integer(gate[0][4], "gate span", language, 0)
        gate_queues = integer(gate[0][5], "gate distinct queue count", language, 0)
        if gate_queues != queues:
            fail(f"GATE row recorded {gate_queues} distinct queues, expected {queues}")
        if gate_span > admission_window_ms:
            fail(f"admission span {gate_span} ms exceeds the {admission_window_ms} ms window")

    first_starts = {}
    for queue_id, records in start_by_queue.items():
        first_starts[queue_id] = min(record[0] for record in records)
    if len(first_starts) == queues:
        span_ms = (max(first_starts.values()) - min(first_starts.values())) // 1_000_000
        if span_ms > admission_window_ms:
            fail(f"first Pull starts span {span_ms} ms, exceeding the "
                 f"{admission_window_ms} ms admission window")
        barrier = max(first_starts.values())
        early = [end for end in ends if end[0] < barrier]
        if early:
            queue_ids = sorted({end[1] for end in early})
            fail(f"a target Pull response completed before every assigned queue had an "
                 f"outstanding Pull (queues {queue_ids})")

    # 5. One active Pull per queue-generation, and follow-ups wait for slot release.
    events = [(stamp, 1, queue_id, opaque) for stamp, queue_id, opaque in starts]
    events += [(stamp, 0, queue_id, opaque) for stamp, queue_id, opaque in ends]
    # At equal timestamps a completion releases the slot before the next admission.
    events.sort(key=lambda event: (event[0], event[1]))
    active = {}
    for _stamp, kind, queue_id, opaque in events:
        if kind == 1:
            if queue_id in active:
                fail(f"queue {queue_id} admitted a second Pull while opaque "
                     f"{active[queue_id]} was still outstanding")
            active[queue_id] = opaque
        else:
            if active.get(queue_id) != opaque:
                fail(f"queue {queue_id} completed opaque {opaque} that was not its active Pull")
            active.pop(queue_id, None)

    # A long-poll that is still outstanding when the client shuts down never sees
    # a response hook. Such an admission is legitimate only as the latest Pull of
    # its queue; every earlier Pull on that queue must have completed.
    end_opaque_set = set(end_opaques)
    in_flight = []
    for queue_id, records in sorted(start_by_queue.items()):
        ordered_starts = sorted(records)
        completions = sorted(end_by_queue.get(queue_id, []))
        if not completions:
            fail(f"queue {queue_id} never completed a Pull")
            continue
        unmatched = [record for record in ordered_starts if record[1] not in end_opaque_set]
        if len(unmatched) > 1:
            fail(f"queue {queue_id} left {len(unmatched)} Pull admissions without a response")
            continue
        if unmatched and unmatched[-1][1] != ordered_starts[-1][1]:
            fail(f"queue {queue_id} completed a later Pull while an earlier one "
                 "stayed outstanding")
        if len(completions) != len(ordered_starts) - len(unmatched):
            fail(f"queue {queue_id} has {len(ordered_starts)} Pull admissions but "
                 f"{len(completions)} completions")
            continue
        if unmatched:
            in_flight.append(queue_id)
        for index in range(1, len(ordered_starts)):
            previous_end = completions[index - 1][0]
            if ordered_starts[index][0] < previous_end:
                fail(f"queue {queue_id} admitted its attempt {index + 1} before the "
                     f"previous Pull completed")

    # 6. Shutdown ordering: no new Pull after the client unregistered and stopped.
    if len(rows["SHUTDOWN"]) != 1 or len(rows["SHUTDOWN"][0]) != 4:
        fail("exactly one well-formed SHUTDOWN row is required")
    else:
        shutdown_at = integer(rows["SHUTDOWN"][0][3], "shutdown timestamp", language, 0)
        late = [start for start in starts if start[0] > shutdown_at]
        if late:
            fail(f"{len(late)} Pull admission(s) happened after client shutdown")

    # 7. Seeded messages: one send and exactly one delivery per assigned queue.
    sends = {}
    for row in rows["SEND_START"]:
        if len(row) != 6:
            fail(f"malformed SEND_START row: {row!r}")
            continue
        stamp = integer(row[3], "send start timestamp", language, 0)
        queue_id = integer(row[4], "send start queue id", language, 0)
        key = row[5] if len(row) > 5 else ""
        if not key:
            fail("SEND_START row has an empty message key")
        if key in sends:
            fail(f"duplicate SEND_START for message key {key!r}")
        sends[key] = (stamp, queue_id)
    if len(sends) != queues:
        fail(f"{len(sends)} seeded sends recorded, expected one per queue ({queues})")
    send_queues = {queue_id for _stamp, queue_id in sends.values()}
    if send_queues != expected_queues:
        fail(f"seeded sends must cover every queue exactly once; saw {sorted(send_queues)}")

    statuses = {}
    for row in rows["SEND_END"]:
        if len(row) != 8:
            fail(f"malformed SEND_END row: {row!r}")
            continue
        key = row[5]
        status = row[6]
        if key not in sends:
            fail(f"SEND_END for unknown message key {key!r}")
        elif status != "SEND_OK":
            fail(f"seeded send for {key!r} returned {status!r} instead of SEND_OK")
        statuses[key] = status

    deliveries = {}
    for row in rows["DELIVERY"]:
        if len(row) != 7:
            fail(f"malformed DELIVERY row: {row!r}")
            continue
        stamp = integer(row[3], "delivery timestamp", language, 0)
        queue_id = integer(row[4], "delivery queue id", language, 0)
        key = row[5]
        if key in deliveries:
            fail(f"message key {key!r} was delivered more than once")
        deliveries[key] = (stamp, queue_id)

    elapsed_ms = {}
    for key, (send_start, send_queue) in sorted(sends.items()):
        if key not in deliveries:
            fail(f"seeded message {key!r} was never delivered")
            continue
        delivery_at, delivery_queue = deliveries[key]
        if delivery_queue != send_queue:
            fail(f"message {key!r} was delivered on queue {delivery_queue} but sent to "
                 f"{send_queue}")
        elapsed = (delivery_at - send_start) // 1_000_000
        if elapsed < 0:
            fail(f"message {key!r} delivery precedes its own send start")
        if elapsed > delivery_window_ms:
            fail(f"queue {send_queue} first delivery took {elapsed} ms, exceeding the "
                 f"{delivery_window_ms} ms delivery window")
        elapsed_ms[send_queue] = elapsed

    unexpected = sorted(set(deliveries) - set(sends))
    if unexpected:
        fail(f"deliveries for unseeded message keys: {unexpected}")

    return failures, elapsed_ms, in_flight


def summarize(language, elapsed_ms, in_flight=()):
    values = [elapsed_ms[queue_id] for queue_id in sorted(elapsed_ms)]
    if not values:
        return f"{language}: no delivery latency samples"
    ordered = sorted(values)
    median = ordered[len(ordered) // 2] if len(ordered) % 2 else (
        ordered[len(ordered) // 2 - 1] + ordered[len(ordered) // 2]) / 2
    per_queue = ", ".join(f"q{queue_id}={elapsed_ms[queue_id]}ms" for queue_id in sorted(elapsed_ms))
    outstanding = ""
    if in_flight:
        outstanding = (f" outstanding_at_shutdown={len(in_flight)}"
                       f"[{','.join(str(queue_id) for queue_id in sorted(in_flight))}]")
    return (f"{language}: send-to-delivery min={min(values)}ms median={median}ms "
            f"max={max(values)}ms [{per_queue}]{outstanding}")


def validate(directory, topic_base, queues=DEFAULT_QUEUES,
             admission_window_ms=DEFAULT_ADMISSION_WINDOW_MS,
             delivery_window_ms=DEFAULT_DELIVERY_WINDOW_MS):
    failures = []
    reports = []
    for language in LANGUAGES:
        try:
            rows = parse_rows(directory, language)
        except EvidenceError as error:
            failures.append(str(error))
            continue
        language_failures, elapsed_ms, in_flight = check_language(
            language, rows, topic_base, queues, admission_window_ms, delivery_window_ms)
        failures.extend(language_failures)
        reports.append(summarize(language, elapsed_ms, in_flight))
    return failures, reports


def parse_options(argv):
    options = {"queues": DEFAULT_QUEUES, "admission_window_ms": DEFAULT_ADMISSION_WINDOW_MS,
               "delivery_window_ms": DEFAULT_DELIVERY_WINDOW_MS}
    positional = []
    index = 0
    while index < len(argv):
        item = argv[index]
        if item.startswith("--") and "=" in item:
            name, value = item[2:].split("=", 1)
            options[name.replace("-", "_")] = int(value)
            index += 1
            continue
        positional.append(item)
        index += 1
    return positional, options


def self_test():
    """Exercise the checker against synthetic evidence for pass and fail classes."""

    base = 1_000_000_000

    def rows(language, queues=DEFAULT_QUEUES, suspend=EXPECTED_SUSPEND_MILLIS, starts=None,
             ends=None, sends=None, deliveries=None, duplicate_active=False,
             shutdown_after=True, outstanding=0):
        out = []

        def emit(kind, *fields):
            out.append("\t".join([TAG, kind, language] + [str(field) for field in fields]))

        topic = f"compat_p10_fanout_{language}"
        for queue_id in range(queues):
            emit("ROUTE", topic, "broker-a", queue_id)
        if starts is None:
            starts = [(base + queue_id * 1_000, queue_id, 100 + queue_id)
                      for queue_id in range(queues)]
        if ends is None:
            ends = [(base + 2_000_000_000 + queue_id, queue_id, 100 + queue_id)
                    for queue_id in range(queues)]
        if sends is None:
            sends = [(base + 6_000_000_000 + queue_id * 1_000, queue_id, f"p10f-q{queue_id}")
                     for queue_id in range(queues)]
        if deliveries is None:
            deliveries = [(base + 7_000_000_000 + queue_id * 1_000, queue_id, f"p10f-q{queue_id}")
                          for queue_id in range(queues)]

        for stamp, queue_id, opaque in sorted(starts):
            emit("PULL_START", stamp, queue_id, opaque, suspend, "127.0.0.1:40911")
        for stamp, queue_id, opaque in sorted(ends):
            emit("PULL_END", stamp, queue_id, opaque, 19)
        if duplicate_active:
            # A second admission while the first Pull for queue 0 is still outstanding.
            emit("PULL_START", base + 1_500, 0, 999, suspend, "127.0.0.1:40911")

        first_starts = {}
        for _stamp, queue_id, _opaque in starts:
            first_starts[queue_id] = min(first_starts.get(queue_id, base + 1 << 60), _stamp)
        span_ms = (max(first_starts.values()) - min(first_starts.values())) // 1_000_000
        emit("GATE", max(first_starts.values()), span_ms, len(first_starts))

        # A long-poll that is still suspended when the client shuts down has a
        # recorded admission but no response hook.
        for index in range(outstanding):
            emit("PULL_START", base + 2_100_000_000 + index * 1_000, 0, 900 + index,
                 suspend, "127.0.0.1:40911")

        for stamp, queue_id, key in sends:
            emit("SEND_START", stamp, queue_id, key)
            emit("SEND_END", stamp + 500_000, queue_id, key, "SEND_OK", 0)
        for stamp, queue_id, key in deliveries:
            emit("DELIVERY", stamp, queue_id, key, 0)
        if duplicate_active:
            emit("PULL_END", base + 2_500_000_000, 0, 999, 19)

        emit("SHUTDOWN", base + 20_000_000_000 if shutdown_after else base + 1_000)
        return "\n".join(out) + "\n"

    late_starts = [(base + queue_id * 1_000, queue_id, 100 + queue_id) for queue_id in range(4)]
    late_starts += [(base + 9_000_000_000 + queue_id * 1_000, queue_id, 100 + queue_id)
                    for queue_id in range(4, DEFAULT_QUEUES)]
    late_ends = [(base + 12_000_000_000 + queue_id, queue_id, 100 + queue_id)
                 for queue_id in range(DEFAULT_QUEUES)]

    cases = [
        ("valid", False, None, {}),
        ("four-pulls-before-deadline", True, "exceeding the 5000 ms admission window",
         {"starts": late_starts, "ends": late_ends}),
        ("response-before-barrier", True, "before every assigned queue had an",
         {"ends": [(base + 500, 0, 100)]
          + [(base + 3_000_000_000 + queue_id, queue_id, 100 + queue_id)
             for queue_id in range(1, DEFAULT_QUEUES)]}),
        ("missing-queue", True, "never started a target Pull",
         {"starts": [(base + queue_id * 1_000, queue_id, 100 + queue_id)
                     for queue_id in range(DEFAULT_QUEUES - 1)],
          "ends": [(base + 2_000_000_000 + queue_id, queue_id, 100 + queue_id)
                   for queue_id in range(DEFAULT_QUEUES - 1)]}),
        ("duplicate-queue", True, "admitted a second Pull while opaque",
         {"duplicate_active": True}),
        ("invalid-timing", True, "exceeding the 5000 ms delivery window",
         {"deliveries": [(base + 30_000_000_000 + queue_id, queue_id, f"p10f-q{queue_id}")
                         for queue_id in range(DEFAULT_QUEUES)]}),
        ("wrong-suspend", True, "expected 15000", {"suspend": 20000}),
        ("pull-after-shutdown", True, "after client shutdown", {"shutdown_after": False}),
        ("outstanding-at-shutdown", False, None, {"outstanding": 1}),
        ("two-outstanding-one-queue", True, "without a response", {"outstanding": 2}),
    ]

    with tempfile.TemporaryDirectory() as directory:
        for name, expect_fail, fragment, overrides in cases:
            for language in LANGUAGES:
                Path(directory, f"{language}-p10-fanout.tsv").write_text(
                    rows(language, **overrides))
            failures, reports = validate(directory, "compat_p10_fanout")
            if not expect_fail and len(reports) != len(LANGUAGES):
                raise SystemExit(f"self-test {name}: expected one report per language: {reports!r}")
            if expect_fail:
                if not failures:
                    raise SystemExit(f"self-test {name}: expected a failure but the checker passed")
                if fragment and not any(fragment in item for item in failures):
                    raise SystemExit(f"self-test {name}: expected {fragment!r} in {failures!r}")
            elif failures:
                raise SystemExit(f"self-test {name}: expected a pass but got {failures!r}")

        # A missing language file must be reported rather than silently skipped.
        Path(directory, "rust-p10-fanout.tsv").unlink()
        failures, _reports = validate(directory, "compat_p10_fanout")
        if not any("missing evidence file" in item for item in failures):
            raise SystemExit(f"self-test missing-file: unexpected result {failures!r}")

    print(f"P10-V05 checker self-test PASS ({len(cases) + 1} synthetic cases)")
    return 0


def main(argv):
    if argv and argv[0] == "--self-test":
        return self_test()
    positional, options = parse_options(argv)
    if len(positional) != 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    directory, topic_base = positional
    failures, reports = validate(
        directory, topic_base, queues=options["queues"],
        admission_window_ms=options["admission_window_ms"],
        delivery_window_ms=options["delivery_window_ms"])
    for report in reports:
        print(report)
    if failures:
        for failure in failures:
            print(f"FAIL {failure}", file=sys.stderr)
        return 1
    print(f"P10-V05 fan-out evidence PASS: {len(LANGUAGES)} clients, "
          f"{options['queues']} queues, admission window {options['admission_window_ms']} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
