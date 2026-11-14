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
    shutdown_at = None
    if len(rows["SHUTDOWN"]) != 1 or len(rows["SHUTDOWN"][0]) != 4:
        fail("exactly one well-formed SHUTDOWN row is required")
    else:
        shutdown_at = integer(rows["SHUTDOWN"][0][3], "shutdown timestamp", language, 0)

    first_starts = {}
    for queue_id, records in start_by_queue.items():
        first_starts[queue_id] = min(record[0] for record in records)
    barrier = max(first_starts.values()) if first_starts else None
    span_ms = ((barrier - min(first_starts.values())) // 1_000_000) if first_starts else 0

    gate = rows["GATE"]
    gate_at = None
    if len(gate) != 1:
        fail(f"exactly one GATE row is required, found {len(gate)}")
    elif len(gate[0]) != 6:
        fail(f"malformed GATE row: {gate[0]!r}")
    else:
        gate_at = integer(gate[0][3], "gate timestamp", language, 0)
        gate_span = integer(gate[0][4], "gate span", language, 0)
        gate_queues = integer(gate[0][5], "gate distinct queue count", language, 0)
        if gate_queues != queues:
            fail(f"GATE row recorded {gate_queues} distinct queues, expected {queues}")
        if gate_span > admission_window_ms:
            fail(f"admission span {gate_span} ms exceeds the {admission_window_ms} ms window")

    if len(first_starts) == queues:
        if span_ms > admission_window_ms:
            fail(f"first Pull starts span {span_ms} ms, exceeding the "
                 f"{admission_window_ms} ms admission window")
        early = [end for end in ends if end[0] < barrier]
        if early:
            queue_ids = sorted({end[1] for end in early})
            fail(f"a target Pull response completed before every assigned queue had an "
                 f"outstanding Pull (queues {queue_ids})")

    # The GATE row must describe the recorded starts, not merely sit somewhere in the log.
    if gate_at is not None and first_starts:
        started_by_gate = {queue_id for queue_id, stamp in first_starts.items() if stamp <= gate_at}
        if len(started_by_gate) != queues:
            missing = sorted(set(first_starts) - started_by_gate)
            fail(f"GATE timestamp precedes the initial Pull start of queues {missing}, so the "
                 f"recorded barrier was not reached when the gate was written")
        if gate_queues != len(started_by_gate):
            fail(f"GATE row recorded {gate_queues} distinct queues but only "
                 f"{len(started_by_gate)} initial Pull starts precede its timestamp")
        if abs(gate_span - span_ms) > 1:
            fail(f"GATE span {gate_span} ms does not match the {span_ms} ms span of the "
                 f"recorded first Pull starts")
        if ends and gate_at > min(end[0] for end in ends):
            fail("a target Pull response completed before the recorded admission gate")
        # The barrier itself must be reached inside the admission window, measured from the
        # earliest recorded start rather than from a span the probe chose to report.
        gate_elapsed_ns = gate_at - min(first_starts.values())
        if gate_elapsed_ns < 0:
            fail(f"GATE timestamp {gate_at} precedes the first Pull start "
                 f"{min(first_starts.values())}, so the recorded clock ordering is impossible")
        elif gate_elapsed_ns > admission_window_ms * 1_000_000:
            fail(f"GATE was reached {gate_elapsed_ns // 1_000_000} ms after the first Pull start, "
                 f"outside the {admission_window_ms} ms admission window")

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
    if shutdown_at is not None:
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
        if gate_at is not None and stamp < gate_at:
            fail(f"SEND_START for message key {key!r} precedes the admission gate, so the "
                 f"topic was not empty while the barrier was measured")
        sends[key] = (stamp, queue_id)
    if len(sends) != queues:
        fail(f"{len(sends)} seeded sends recorded, expected one per queue ({queues})")
    send_queues = {queue_id for _stamp, queue_id in sends.values()}
    if send_queues != expected_queues:
        fail(f"seeded sends must cover every queue exactly once; saw {sorted(send_queues)}")

    # Every seeded send needs exactly one successful completion that matches its queue,
    # with a non-negative offset and an end stamp at/after its start. A delivery may
    # still precede the send return (the listener can run on another thread).
    completions = {}
    for row in rows["SEND_END"]:
        if len(row) != 8:
            fail(f"malformed SEND_END row: {row!r}")
            continue
        stamp = integer(row[3], "send end timestamp", language, 0)
        queue_id = integer(row[4], "send end queue id", language, 0)
        key = row[5]
        status = row[6]
        if key not in sends:
            fail(f"SEND_END for unknown message key {key!r}")
            continue
        if key in completions:
            fail(f"duplicate SEND_END for message key {key!r}")
            continue
        send_start, send_queue = sends[key]
        if status != "SEND_OK":
            fail(f"seeded send for {key!r} returned {status!r} instead of SEND_OK")
        if queue_id != send_queue:
            fail(f"SEND_END for message key {key!r} names queue {queue_id} but the send "
                 f"target was queue {send_queue}")
        offset = integer(row[7], "send end queue offset", language, 0)
        if offset < 0:
            fail(f"SEND_END for message key {key!r} reported negative offset {offset}")
        if stamp < send_start:
            fail(f"SEND_END for message key {key!r} precedes its own SEND_START")
        completions[key] = (stamp, queue_id, status, offset)
    missing_end = sorted(set(sends) - set(completions))
    if missing_end:
        fail(f"seeded send(s) without a matching SEND_END row: {missing_end}")

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
        if shutdown_at is not None and delivery_at > shutdown_at:
            fail(f"message {key!r} was delivered after the client shut down")
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
             shutdown_after=True, outstanding=0, gate_at=None, send_before_gate=False,
             drop_send_end=False, duplicate_send_end=False, mismatched_send_end=False,
             delivery_before_send_return=False, duplicate_delivery=False,
             delivery_after_shutdown=False):
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
            send_base = base - 1_000 if send_before_gate else base + 6_000_000_000
            sends = [(send_base + queue_id * 1_000, queue_id, f"p10f-q{queue_id}")
                     for queue_id in range(queues)]
        if deliveries is None:
            if delivery_after_shutdown:
                deliveries = [(base + 20_001_000_000 + queue_id * 1_000, queue_id,
                               f"p10f-q{queue_id}") for queue_id in range(queues)]
            else:
                deliveries = [(base + 7_000_000_000 + queue_id * 1_000, queue_id,
                               f"p10f-q{queue_id}") for queue_id in range(queues)]

        for stamp, queue_id, opaque in sorted(starts):
            emit("PULL_START", stamp, queue_id, opaque, suspend, "127.0.0.1:40911")
        for stamp, queue_id, opaque in sorted(ends):
            emit("PULL_END", stamp, queue_id, opaque, 19)
        if duplicate_active:
            # A second admission while the first Pull for queue 0 is still outstanding.
            emit("PULL_START", base + 1_500, 0, 999, suspend, "127.0.0.1:40911")

        first_starts = {}
        for stamp, queue_id, _opaque in starts:
            first_starts[queue_id] = min(first_starts.get(queue_id, base + 1 << 60), stamp)
        span_ms = (max(first_starts.values()) - min(first_starts.values())) // 1_000_000
        emit("GATE", gate_at if gate_at is not None else max(first_starts.values()),
             span_ms, len(first_starts))

        # A long-poll that is still suspended when the client shuts down has a
        # recorded admission but no response hook.
        for index in range(outstanding):
            emit("PULL_START", base + 2_100_000_000 + index * 1_000, 0, 900 + index,
                 suspend, "127.0.0.1:40911")

        for stamp, queue_id, key in sends:
            emit("SEND_START", stamp, queue_id, key)
            if delivery_before_send_return:
                # The listener callback can run before the send call returns.
                emit("DELIVERY", stamp + 100_000, queue_id, key, 0)
            if drop_send_end:
                continue
            end_queue = 99 if mismatched_send_end else queue_id
            emit("SEND_END", stamp + 500_000, end_queue, key, "SEND_OK", 0)
            if duplicate_send_end and queue_id == 0:
                emit("SEND_END", stamp + 600_000, queue_id, key, "SEND_OK", 1)
        if not delivery_before_send_return:
            for stamp, queue_id, key in deliveries:
                emit("DELIVERY", stamp, queue_id, key, 0)
        if duplicate_delivery:
            emit("DELIVERY", base + 7_000_500_000, 0, "p10f-q0", 0)
        if duplicate_active:
            emit("PULL_END", base + 2_500_000_000, 0, 999, 19)

        emit("SHUTDOWN", base + 20_000_000_000 if shutdown_after else base + 1_000)
        return "\n".join(out) + "\n"

    near_shutdown_sends = [(base + 19_000_000_000 + queue_id * 1_000, queue_id,
                            f"p10f-q{queue_id}") for queue_id in range(DEFAULT_QUEUES)]

    late_starts = [(base + queue_id * 1_000, queue_id, 100 + queue_id) for queue_id in range(4)]
    late_starts += [(base + 9_000_000_000 + queue_id * 1_000, queue_id, 100 + queue_id)
                    for queue_id in range(4, DEFAULT_QUEUES)]
    late_ends = [(base + 12_000_000_000 + queue_id, queue_id, 100 + queue_id)
                 for queue_id in range(DEFAULT_QUEUES)]

    # A gate that is latched long after a compact set of starts: the start span stays inside the
    # window, so only an elapsed-gate check can reject it. Responses and sends stay after the gate
    # so this exercises the elapsed limit rather than another ordering rule.
    slow_starts = [(base + 1_000_000_000 + queue_id * 1_000, queue_id, 100 + queue_id)
                   for queue_id in range(DEFAULT_QUEUES)]
    slow_ends = [(base + 12_000_000_000 + queue_id, queue_id, 100 + queue_id)
                 for queue_id in range(DEFAULT_QUEUES)]
    slow_sends = [(base + 13_000_000_000 + queue_id * 1_000, queue_id, f"p10f-q{queue_id}")
                  for queue_id in range(DEFAULT_QUEUES)]
    slow_deliveries = [(base + 14_000_000_000 + queue_id * 1_000, queue_id, f"p10f-q{queue_id}")
                       for queue_id in range(DEFAULT_QUEUES)]
    at_limit_gate = base + 5_000_000_000
    over_gate = base + 5_000_000_001
    at_limit_ends = [(base + 6_000_000_000 + queue_id, queue_id, 100 + queue_id)
                     for queue_id in range(DEFAULT_QUEUES)]
    at_limit_sends = [(base + 7_000_000_000 + queue_id * 1_000, queue_id, f"p10f-q{queue_id}")
                      for queue_id in range(DEFAULT_QUEUES)]
    at_limit_deliveries = [(base + 8_000_000_000 + queue_id * 1_000, queue_id,
                            f"p10f-q{queue_id}") for queue_id in range(DEFAULT_QUEUES)]

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
        ("send-end-rows-removed", True, "without a matching SEND_END row",
         {"drop_send_end": True}),
        ("publish-before-gate", True, "precedes the admission gate",
         {"send_before_gate": True,
          "deliveries": [(base + 200_000 + queue_id * 1_000, queue_id, f"p10f-q{queue_id}")
                         for queue_id in range(DEFAULT_QUEUES)]}),
        ("gate-before-final-initial-start", True, "GATE timestamp precedes the initial Pull start",
         {"gate_at": base}),
        ("gate-latched-after-window", True, "outside the 5000 ms admission window",
         {"starts": slow_starts, "ends": slow_ends, "sends": slow_sends,
          "deliveries": slow_deliveries, "gate_at": base + 11_000_000_000}),
        ("gate-exactly-at-window-limit", False, None,
         {"gate_at": at_limit_gate, "ends": at_limit_ends, "sends": at_limit_sends,
          "deliveries": at_limit_deliveries}),
        ("gate-one-nanosecond-past-limit", True, "outside the 5000 ms admission window",
         {"gate_at": over_gate, "ends": at_limit_ends, "sends": at_limit_sends,
          "deliveries": at_limit_deliveries}),
        ("response-between-final-start-and-gate", True,
         "completed before the recorded admission gate",
         {"gate_at": base + 3_000_000_000}),
        ("duplicate-send-end", True, "duplicate SEND_END for message key",
         {"duplicate_send_end": True}),
        ("mismatched-send-end-queue", True, "but the send target was queue",
         {"mismatched_send_end": True}),
        ("duplicate-delivery-row", True, "was delivered more than once",
         {"duplicate_delivery": True}),
        ("delivery-after-shutdown", True, "delivered after the client shut down",
         {"sends": near_shutdown_sends, "delivery_after_shutdown": True}),
        ("callback-before-send-return", False, None,
         {"delivery_before_send_return": True}),
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
