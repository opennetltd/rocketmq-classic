#!/usr/bin/env python3
"""Compare normalized JSON results without discarding meaningful array order."""

import json
import os
import re
import sys
import tempfile
from pathlib import Path


def normalize(value, unordered_paths=frozenset(), path="$", ignored=frozenset()):
    if isinstance(value, dict):
        return {
            key: normalize(item, unordered_paths, f"{path}.{key}", ignored)
            for key, item in value.items()
            if key not in ignored
        }
    if isinstance(value, list):
        items = [normalize(item, unordered_paths, f"{path}[]", ignored) for item in value]
        if path in unordered_paths:
            return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
        return items
    return value


def compare(left, right, unordered_paths=frozenset(), ignored=frozenset()):
    left_normal = normalize(left, unordered_paths, ignored=ignored)
    right_normal = normalize(right, unordered_paths, ignored=ignored)
    return left_normal == right_normal


def pending_phase19_source_case_ids():
    pending = set()
    for plan_path in Path("plan/2026/09").glob("*-phase-*.md"):
        if plan_path.name.endswith(".zh-tw.md"):
            continue
        match = re.search(r"phase-(\d+)-", plan_path.name)
        if not match:
            continue
        phase = int(match.group(1))
        if phase > 17:
            continue
        plan = plan_path.read_text()
        if phase <= 11:
            # These early plans record their pending live gates as unchecked
            # validation rows rather than in a dedicated Phase 19 section.
            pending.update(re.findall(r"(?m)^- \[ \] \*\*(P\d+-V\d+)\*\*", plan))
            continue
        handoff = re.search(
            r"(?ms)^### [^\n]*Phase 19 live-test handoff[^\n]*\n(.*?)(?=^## |\Z)",
            plan,
        )
        if handoff:
            pending.update(
                re.findall(r"(?m)^- \[ \] \*\*(P\d+-V\d+)\*\*", handoff.group(1))
            )
    return pending


def configured_evidence_root():
    configured = os.environ.get("ROCKETMQ_CLASSIC_EVIDENCE_DIR")
    if configured:
        return Path(configured).resolve()
    return (Path(tempfile.gettempdir()) / "rocketmq-classic-test-artifacts" / "phase-19").resolve()


def configured_manifest_path():
    configured = os.environ.get("ROCKETMQ_CLASSIC_SCENARIOS_MANIFEST")
    if configured:
        return Path(configured)
    return Path("tests/differential/scenarios.yaml")


def validate_manifest(manifest):
    if manifest.get("java_client_version") != "5.5.1" or manifest.get("server_version") != "5.3.4":
        raise ValueError("manifest client/server versions do not match pinned baselines")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("manifest scenarios must be a non-empty array")
    ids = set()
    for scenario in scenarios:
        required = {
            "id",
            "phase",
            "execution_phase",
            "profile",
            "inputs",
            "assertions",
            "client_directions",
            "cleanup",
            "normalization",
            "live_status",
        }
        if not isinstance(scenario, dict) or not required.issubset(scenario):
            raise ValueError("scenario is missing required fields")
        if scenario["id"] in ids:
            raise ValueError(f"duplicate scenario id: {scenario['id']}")
        ids.add(scenario["id"])
        assertions = scenario["assertions"]
        if not isinstance(assertions, list) or not assertions or not all(
            isinstance(assertion, str) and assertion.strip()
            for assertion in assertions
        ):
            raise ValueError(f"scenario has no explicit assertions: {scenario['id']}")
        checks = scenario["inputs"].get("checks", [])
        missing_assertions = set(checks) - set(assertions)
        if missing_assertions:
            missing = ", ".join(sorted(missing_assertions))
            raise ValueError(
                f"scenario assertions omit input checks for {scenario['id']}: {missing}"
            )
        directions = scenario["client_directions"]
        if (
            not isinstance(directions, list)
            or not directions
            or not all(isinstance(direction, str) and direction.strip() for direction in directions)
            or not any("java" in direction.lower() for direction in directions)
            or not any("rust" in direction.lower() for direction in directions)
        ):
            raise ValueError(
                f"scenario must declare Java and Rust client directions: {scenario['id']}"
            )
        cleanup = scenario["cleanup"]
        cleanup_fields = {"topics", "consumer_groups", "offsets_with_groups", "notes"}
        if not isinstance(cleanup, dict) or not cleanup_fields.issubset(cleanup):
            raise ValueError(f"scenario is missing cleanup instructions: {scenario['id']}")
        if (
            not isinstance(cleanup["topics"], list)
            or not isinstance(cleanup["consumer_groups"], list)
            or cleanup["offsets_with_groups"] is not True
            or not isinstance(cleanup["notes"], str)
            or not cleanup["notes"].strip()
        ):
            raise ValueError(f"scenario has invalid cleanup instructions: {scenario['id']}")
        input_resources = set()
        for name in ("topic", "group", "consumer_group", "producer_group"):
            value = scenario["inputs"].get(name)
            if isinstance(value, str):
                input_resources.add(value)
        for name in ("producer_groups", "topics", "groups"):
            value = scenario["inputs"].get(name, [])
            if isinstance(value, list):
                input_resources.update(item for item in value if isinstance(item, str))
        cleanup_resources = set(cleanup["topics"]) | set(cleanup["consumer_groups"])
        if not input_resources.issubset(cleanup_resources):
            missing = ", ".join(sorted(input_resources - cleanup_resources))
            raise ValueError(
                f"scenario cleanup omits input resources for {scenario['id']}: {missing}"
            )
        if not all(
            isinstance(resource, str) and resource.startswith("compat_")
            for resource in cleanup_resources
        ):
            raise ValueError(
                f"scenario cleanup includes a non-isolated resource: {scenario['id']}"
            )
        live_status = scenario["live_status"]
        if live_status not in {"PENDING_PHASE_19", "PASS", "PASS_RECORDED"}:
            raise ValueError(f"unsupported live status for {scenario['id']}: {live_status}")
        if live_status == "PASS_RECORDED":
            if scenario["execution_phase"] != 19:
                raise ValueError(f"only Phase 19 scenarios may carry live PASS: {scenario['id']}")
            if scenario.get("evidence") is not None:
                raise ValueError(f"recorded Phase 19 outcomes must not include test artifacts: {scenario['id']}")
            continue
        if live_status == "PASS":
            if scenario["execution_phase"] != 19:
                raise ValueError(f"only Phase 19 scenarios may carry live PASS: {scenario['id']}")
            evidence = scenario.get("evidence")
            if not isinstance(evidence, str) or not evidence:
                raise ValueError(f"Phase 19 PASS requires evidence: {scenario['id']}")
            evidence_root = configured_evidence_root()
            evidence_path = Path(evidence)
            if not evidence_path.is_absolute():
                evidence_path = evidence_root / evidence_path
            evidence_path = evidence_path.resolve()
            if evidence_root not in evidence_path.parents or not evidence_path.is_dir():
                raise ValueError(f"Phase 19 evidence path is missing or outside the evidence root: {scenario['id']}")
            metadata_path = evidence_path / "metadata.txt"
            result_path = evidence_path / "result.txt"
            if not metadata_path.is_file() or not result_path.is_file():
                raise ValueError(f"Phase 19 evidence is incomplete: {scenario['id']}")
            if "exit_status=0" not in metadata_path.read_text() or f"PASS {scenario['id']}:" not in result_path.read_text():
                raise ValueError(f"Phase 19 evidence does not record a successful run: {scenario['id']}")
            if scenario["id"] == "ROUTE-001-get-existing-topic":
                java_rows = evidence_path / "java-route.tsv"
                rust_rows = evidence_path / "rust-route.tsv"
                if not java_rows.is_file() or not rust_rows.is_file() or java_rows.read_bytes() != rust_rows.read_bytes():
                    raise ValueError(f"Phase 19 route evidence does not match: {scenario['id']}")
            if scenario["id"] == "ROUTE-002-topic-not-exist":
                java_rows = evidence_path / "java-route.tsv"
                rust_rows = evidence_path / "rust-route.tsv"
                expected = f"NO_ROUTE\t{scenario['inputs']['topic']}\n".encode()
                if (
                    not java_rows.is_file()
                    or not rust_rows.is_file()
                    or java_rows.read_bytes() != expected
                    or rust_rows.read_bytes() != expected
                ):
                    raise ValueError(f"Phase 19 missing-route evidence does not match: {scenario['id']}")
            if scenario["id"] in {
                "PROD-001-sync-send",
                "PROD-002-async-send",
                "PROD-003-oneway-send",
                "PROD-004-batch-send",
            }:
                topic = scenario["inputs"].get("topic")
                body = scenario["inputs"].get("body")
                if not isinstance(topic, str) or not isinstance(body, str):
                    raise ValueError("producer-send evidence requires topic and body inputs")
                expected_body = body.encode().hex()

                def tsv_rows(name, columns=None):
                    path = evidence_path / name
                    if not path.is_file():
                        raise ValueError(f"producer-send evidence is missing {name}")
                    rows = [line.split("\t") for line in path.read_text().splitlines()]
                    if not rows or (
                        columns is not None and any(len(row) != columns for row in rows)
                    ):
                        raise ValueError(f"producer-send evidence has invalid rows in {name}")
                    return rows

                def one_tsv_row(name, columns):
                    rows = tsv_rows(name, columns)
                    if len(rows) != 1:
                        raise ValueError(f"producer-send evidence must have one row in {name}")
                    return rows[0]

                asynchronous = scenario["id"] == "PROD-002-async-send"
                oneway = scenario["id"] == "PROD-003-oneway-send"
                batch = scenario["id"] == "PROD-004-batch-send"
                if batch:
                    message_count = scenario["inputs"].get("messages")
                    if not isinstance(message_count, int) or message_count < 1:
                        raise ValueError("batch send evidence requires a positive message count")
                    java_send = one_tsv_row("java-batch-send.tsv", 9)
                    rust_send = one_tsv_row("rust-batch-send.tsv", 9)
                    for language, row in (("Java", java_send), ("Rust", rust_send)):
                        offset_ids = row[5].split(",")
                        if (
                            row[0] != "BATCH_SEND"
                            or row[1] != "SEND_OK"
                            or row[2] != topic
                            or row[3] != "0"
                            or not row[4].isdecimal()
                            or len(offset_ids) != message_count
                            or offset_ids[0] != row[6]
                            or any(
                                not re.fullmatch(r"[0-9a-fA-F]+", message_id)
                                or len(message_id) not in {32, 56}
                                for message_id in offset_ids
                            )
                            or not row[7].isdecimal()
                            or int(row[6][-16:], 16) != int(row[7])
                            or row[8] != str(message_count)
                        ):
                            raise ValueError(f"{language} batch-send result is invalid")
                    if (
                        java_send[1:4] != rust_send[1:4]
                        or int(rust_send[4]) != int(java_send[4]) + message_count
                    ):
                        raise ValueError("Java and Rust batch-send results or queue progression differ")
                    expected_bodies = [
                        f"{body}-{index}".encode().hex()
                        for index in range(message_count)
                    ]
                    for readback_name, send_result in (
                        ("rust-read-java.tsv", java_send),
                        ("java-read-rust.tsv", rust_send),
                    ):
                        rows = tsv_rows(readback_name, 5)
                        if len(rows) != message_count:
                            raise ValueError(
                                f"batch readback must contain {message_count} rows in {readback_name}"
                            )
                        for index, row in enumerate(rows):
                            if (
                                row[0] != "MESSAGE"
                                or row[1] != topic
                                or row[2] != "0"
                                or row[3] != str(int(send_result[4]) + index)
                                or row[4] != expected_bodies[index]
                            ):
                                raise ValueError(
                                    f"batch cross-language readback does not match {readback_name}"
                                )
                elif oneway:
                    java_rows = tsv_rows("java-oneway.tsv")
                    rust_rows = tsv_rows("rust-oneway.tsv")
                    inputs = scenario["inputs"]
                    key_prefix = inputs.get("key_prefix")
                    if (
                        len(java_rows) != 2
                        or len(rust_rows) != 2
                        or len(java_rows[0]) != 4
                        or len(rust_rows[0]) != 4
                        or len(java_rows[1]) != 2
                        or len(rust_rows[1]) != 2
                    ):
                        raise ValueError("oneway evidence must contain a return row and queue offset result")
                    java_return, java_max = java_rows
                    rust_return, rust_max = rust_rows
                    if not isinstance(key_prefix, str) or not re.fullmatch(
                        r"compat_[a-z0-9_]+", key_prefix
                    ):
                        raise ValueError("oneway send evidence requires an isolated key prefix")
                    java_expected_max = inputs.get("java_expected_max_offset")
                    rust_expected_max = inputs.get("rust_expected_max_offset")
                    java_expected_queue = inputs.get("java_expected_queue_offset")
                    rust_expected_queue = inputs.get("rust_expected_queue_offset")
                    if (
                        not all(isinstance(value, int) and value >= 0 for value in (
                            java_expected_max,
                            rust_expected_max,
                            java_expected_queue,
                            rust_expected_queue,
                        ))
                        or java_expected_max <= 0
                        or rust_expected_max <= java_expected_max
                    ):
                        raise ValueError("oneway expected Broker queue offsets are invalid")
                    if (
                        len(java_return) != 4
                        or java_return[:3] != ["ONEWAY_RETURN", "NO_RESPONSE", topic]
                        or not java_return[3].startswith(f"{key_prefix}_java_")
                    ):
                        raise ValueError("Java oneway send did not return without a Broker response")
                    if (
                        len(rust_return) != 4
                        or rust_return[:3] != ["ONEWAY_RETURN", "NO_RESPONSE", topic]
                        or not rust_return[3].startswith(f"{key_prefix}_rust_")
                        or rust_return[3] == java_return[3]
                    ):
                        raise ValueError("Rust oneway send did not return without a Broker response")
                    if (
                        java_max != ["QUEUE_MAX_OFFSET", str(java_expected_max)]
                        or rust_max != ["QUEUE_MAX_OFFSET", str(rust_expected_max)]
                    ):
                        raise ValueError("oneway sends did not advance the Broker queue as expected")
                    java_message_id = one_tsv_row("java-message-id.tsv", 5)
                    rust_message_id = one_tsv_row("rust-message-id.tsv", 5)
                    for language, row, message_key, expected_offset in (
                        ("Java", java_message_id, java_return[3], java_expected_queue),
                        ("Rust", rust_message_id, rust_return[3], rust_expected_queue),
                    ):
                        key_query = one_tsv_row(f"{language.lower()}-key-query.tsv", 5)
                        if (
                            row[0] != "MESSAGE_ID"
                            or row[1] != message_key
                            or not re.fullmatch(r"[0-9a-fA-F]+", row[2])
                            or len(row[2]) not in {32, 56}
                            or row[3] != "0"
                            or row[4] != str(expected_offset)
                        ):
                            raise ValueError(f"{language} oneway Broker key lookup is invalid")
                        if (
                            key_query[0] != "KEY_QUERY"
                            or key_query[1] != message_key
                            or key_query[2] != f"{message_key}_uniq"
                            or key_query[3] != row[3]
                            or key_query[4] != row[4]
                        ):
                            raise ValueError(f"{language} oneway unique-key lookup is invalid")
                        offset_lines = (
                            evidence_path / f"broker-offset-query-{language.lower()}.txt"
                        ).read_text().splitlines()
                        offset_values = {
                            name.strip(): value.strip()
                            for line in offset_lines
                            if ":" in line
                            for name, value in [line.split(":", 1)]
                        }
                        if (
                            offset_values.get("OffsetID") != row[2]
                            or offset_values.get("Queue ID") != row[3]
                            or offset_values.get("Queue Offset") != row[4]
                        ):
                            raise ValueError(f"{language} physical offset ID evidence is inconsistent")
                    if java_message_id[2] == rust_message_id[2]:
                        raise ValueError("Java and Rust oneway sends resolved to the same message ID")
                    for language in ("java", "rust"):
                        for raw_name in (
                            f"broker-query-{language}.txt",
                            f"broker-offset-query-{language}.txt",
                        ):
                            raw_query = evidence_path / raw_name
                            if not raw_query.is_file() or not raw_query.read_text().strip():
                                raise ValueError(f"oneway Broker query output is missing: {raw_name}")
                    readback_sends = (
                        ("rust-read-java.tsv", java_message_id, 4),
                        ("java-read-rust.tsv", rust_message_id, 4),
                    )
                elif asynchronous:
                    java_send = one_tsv_row("java-async-send.tsv", 8)
                    rust_send = one_tsv_row("rust-async-send.tsv", 8)
                    expected_shapes = (
                        ("Java", java_send, "ASYNC_CALLBACK"),
                        ("Rust", rust_send, "ASYNC_FUTURE"),
                    )
                    for language, row, completion_kind in expected_shapes:
                        if row[0] != completion_kind or row[1] != "1":
                            raise ValueError(
                                f"{language} asynchronous send must complete exactly once"
                            )
                        if (
                            row[2] != "SEND_OK"
                            or row[3] != topic
                            or row[4] != "0"
                            or not row[5].isdecimal()
                            or not re.fullmatch(r"[0-9a-fA-F]+", row[6])
                            or len(row[6]) not in {32, 56}
                            or not row[7].isdecimal()
                            or int(row[6][-16:], 16) != int(row[7])
                        ):
                            raise ValueError(f"{language} asynchronous send result is invalid")
                    if java_send[2:5] != rust_send[2:5]:
                        raise ValueError("Java and Rust asynchronous send status/queue results differ")
                    readback_sends = (
                        ("rust-read-java.tsv", java_send, 5),
                        ("java-read-rust.tsv", rust_send, 5),
                    )
                else:
                    java_send = one_tsv_row("java-send.tsv", 7)
                    rust_send = one_tsv_row("rust-send.tsv", 7)
                    for language, row in (("Java", java_send), ("Rust", rust_send)):
                        if (
                            row[0] != "SEND"
                            or row[1] != "SEND_OK"
                            or row[2] != topic
                            or row[3] != "0"
                            or not row[4].isdecimal()
                            or not re.fullmatch(r"[0-9a-fA-F]+", row[5])
                            or len(row[5]) not in {32, 56}
                            or not row[6].isdecimal()
                            or int(row[5][-16:], 16) != int(row[6])
                        ):
                            raise ValueError(f"{language} synchronous-send result is invalid")
                    if java_send[1:4] != rust_send[1:4]:
                        raise ValueError("Java and Rust synchronous-send status/queue results differ")
                    readback_sends = (
                        ("rust-read-java.tsv", java_send, 4),
                        ("java-read-rust.tsv", rust_send, 4),
                    )
                if not batch:
                    for readback_name, send_result, queue_offset_index in readback_sends:
                        row = one_tsv_row(readback_name, 5)
                        if (
                            row[0] != "MESSAGE"
                            or row[1] != topic
                            or row[2] != "0"
                            or not row[3].isdecimal()
                            or row[3] != send_result[queue_offset_index]
                            or row[4] != expected_body
                        ):
                            raise ValueError(f"cross-language readback does not match {readback_name}")
    phase19_ids = {
        scenario["id"]
        for scenario in scenarios
        if scenario["execution_phase"] == 19
    }
    phase19_source_ids = {
        scenario.get("source_case_id", scenario["id"])
        for scenario in scenarios
        if scenario["execution_phase"] == 19
    }
    covered_source_ids = phase19_ids | phase19_source_ids
    missing_source_ids = {
        source_id
        for source_id in pending_phase19_source_case_ids()
        if source_id not in covered_source_ids
        and not any(case_id.startswith(f"{source_id}-") for case_id in phase19_ids)
    }
    if missing_source_ids:
        missing = ", ".join(sorted(missing_source_ids))
        raise ValueError(f"manifest missing Phase 19 source case IDs: {missing}")
    for source_id in pending_phase19_source_case_ids():
        source_phase = int(source_id.split("-")[0][1:])
        source_cases = [
            scenario
            for scenario in scenarios
            if scenario.get("source_case_id", scenario["id"]) == source_id
            or scenario["id"] == source_id
            or scenario["id"].startswith(f"{source_id}-")
        ]
        misplaced = [
            scenario["id"]
            for scenario in source_cases
            if scenario["phase"] != source_phase or scenario["execution_phase"] != 19
        ]
        if misplaced:
            cases = ", ".join(sorted(misplaced))
            raise ValueError(f"Phase 19 source case {source_id} is assigned to the wrong phase: {cases}")
    return len(scenarios)


def self_test():
    assert compare({"a": 1, "b": [1, 2]}, {"b": [1, 2], "a": 1})
    assert not compare({"queue": [1, 2]}, {"queue": [2, 1]})
    assert compare({"queue": [1, 2]}, {"queue": [2, 1]}, frozenset({"$.queue"}))
    assert not compare({"code": 10}, {"code": 11})
    assert compare({"opaque": 1}, {"opaque": 2}, ignored=frozenset({"opaque"}))
    delivery = {
        "status": "SEND_OK",
        "attempts": ["broker-a", "broker-b"],
        "retry_count": 1,
        "messages": [
            {"business_key": "order-7", "queue_offset": 41},
            {"business_key": "order-7", "queue_offset": 42},
        ],
    }
    assert not compare(delivery, {**delivery, "status": "SEND_FAILED"})
    assert not compare(
        delivery,
        {**delivery, "attempts": ["broker-b", "broker-a"]},
    )
    assert not compare(delivery, {**delivery, "retry_count": 2})
    assert not compare(
        delivery,
        {
            **delivery,
            "messages": [
                delivery["messages"][0],
                {"business_key": "order-8", "queue_offset": 42},
            ],
        },
    )
    assert not compare(
        delivery,
        {
            **delivery,
            "messages": [
                delivery["messages"][0],
                {"business_key": "order-7", "queue_offset": 41},
            ],
        },
    )
    manifest = {
        "java_client_version": "5.5.1",
        "server_version": "5.3.4",
        "scenarios": [
            {
                "id": source_id,
                "phase": int(source_id.split("-")[0][1:]),
                "execution_phase": 19,
                "profile": "basic",
                "inputs": {},
                "assertions": ["java-rust-result-equality"],
                "client_directions": [
                    "java-5.5.1-client->server-5.3.4",
                    "rust-client->server-5.3.4",
                ],
                "cleanup": {
                    "topics": [],
                    "consumer_groups": [],
                    "offsets_with_groups": True,
                    "notes": "Delete only resources owned by this scenario.",
                },
                "normalization": [],
                "live_status": "PENDING_PHASE_19",
            }
            for source_id in sorted(pending_phase19_source_case_ids())
        ],
    }
    assert validate_manifest(manifest) == len(pending_phase19_source_case_ids())
    invalid = {
        **manifest,
        "scenarios": [{**manifest["scenarios"][0], "live_status": "PASS"}],
    }
    try:
        validate_manifest(invalid)
        raise AssertionError("pending scenario incorrectly accepted as PASS")
    except ValueError as error:
        assert "requires evidence" in str(error)
    invalid_evidence_path = {
        **manifest,
        "scenarios": [
            {
                **manifest["scenarios"][0],
                "live_status": "PASS",
                "evidence": "../../README.md",
            }
        ],
    }
    try:
        validate_manifest(invalid_evidence_path)
        raise AssertionError("manifest accepted evidence outside the Phase 19 evidence directory")
    except ValueError as error:
        assert "evidence path" in str(error)
    checked_in = json.loads(configured_manifest_path().read_text())
    validate_manifest(checked_in)
    async_scenario = next(
        scenario
        for scenario in checked_in["scenarios"]
        if scenario["id"] == "PROD-002-async-send"
    )
    evidence_root = configured_evidence_root()
    evidence_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="compare-self-test-", dir=evidence_root) as temporary:
        evidence_dir = Path(temporary)
        evidence_rel = str(evidence_dir.resolve())
        (evidence_dir / "metadata.txt").write_text("exit_status=0\n")
        (evidence_dir / "result.txt").write_text(
            "PASS PROD-002-async-send: synthetic validator evidence\n"
        )
        async_outputs = {
            "java-async-send.tsv": (
                "ASYNC_CALLBACK\t1\tSEND_OK\tcompat_phase0_send\t0\t5\t"
                f"{1:032x}\t1\n"
            ),
            "rust-async-send.tsv": (
                "ASYNC_FUTURE\t1\tSEND_OK\tcompat_phase0_send\t0\t6\t"
                f"{2:032x}\t2\n"
            ),
            "rust-read-java.tsv": "MESSAGE\tcompat_phase0_send\t0\t5\t6669786564\n",
            "java-read-rust.tsv": "MESSAGE\tcompat_phase0_send\t0\t6\t6669786564\n",
        }
        for filename, contents in async_outputs.items():
            (evidence_dir / filename).write_text(contents)

        def checked_manifest(java_result):
            changed_scenario = {
                **async_scenario,
                "live_status": "PASS",
                "evidence": evidence_rel,
            }
            changed_scenarios = [
                changed_scenario if row["id"] == async_scenario["id"] else row
                for row in checked_in["scenarios"]
            ]
            manifest = {**checked_in, "scenarios": changed_scenarios}
            if java_result is not None:
                (evidence_dir / "java-async-send.tsv").write_text(java_result)
            return manifest

        validate_manifest(checked_manifest(None))
        try:
            validate_manifest(
                checked_manifest(
                    async_outputs["java-async-send.tsv"].replace(
                        "ASYNC_CALLBACK\t1\t", "ASYNC_CALLBACK\t2\t"
                    )
                )
            )
            raise AssertionError("asynchronous send accepted a duplicate Java callback")
        except ValueError as error:
            assert "Java asynchronous send must complete exactly once" in str(error)
    oneway_scenario = next(
        scenario
        for scenario in checked_in["scenarios"]
        if scenario["id"] == "PROD-003-oneway-send"
    )
    with tempfile.TemporaryDirectory(prefix="compare-self-test-", dir=evidence_root) as temporary:
        evidence_dir = Path(temporary)
        evidence_rel = str(evidence_dir.resolve())
        (evidence_dir / "metadata.txt").write_text("exit_status=0\n")
        (evidence_dir / "result.txt").write_text(
            "PASS PROD-003-oneway-send: synthetic validator evidence\n"
        )
        java_key = "compat_phase0_oneway_java_selftest"
        rust_key = "compat_phase0_oneway_rust_selftest"
        java_message_id = f"{1:032x}"
        rust_message_id = f"{2:032x}"
        oneway_outputs = {
            "java-oneway.tsv": f"ONEWAY_RETURN\tNO_RESPONSE\tcompat_phase0_send\t{java_key}\nQUEUE_MAX_OFFSET\t1\n",
            "rust-oneway.tsv": f"ONEWAY_RETURN\tNO_RESPONSE\tcompat_phase0_send\t{rust_key}\nQUEUE_MAX_OFFSET\t2\n",
            "java-message-id.tsv": f"MESSAGE_ID\t{java_key}\t{java_message_id}\t0\t0\n",
            "rust-message-id.tsv": f"MESSAGE_ID\t{rust_key}\t{rust_message_id}\t0\t1\n",
            "java-key-query.tsv": f"KEY_QUERY\t{java_key}\t{java_key}_uniq\t0\t0\n",
            "rust-key-query.tsv": f"KEY_QUERY\t{rust_key}\t{rust_key}_uniq\t0\t1\n",
            "broker-query-java.txt": f"#Message ID\t#QID\t#Offset\n{java_message_id}\t0\t0\n",
            "broker-query-rust.txt": f"#Message ID\t#QID\t#Offset\n{rust_message_id}\t0\t1\n",
            "broker-offset-query-java.txt": f"OffsetID: {java_message_id}\nQueue ID: 0\nQueue Offset: 0\n",
            "broker-offset-query-rust.txt": f"OffsetID: {rust_message_id}\nQueue ID: 0\nQueue Offset: 1\n",
            "rust-read-java.tsv": "MESSAGE\tcompat_phase0_send\t0\t0\t6669786564\n",
            "java-read-rust.tsv": "MESSAGE\tcompat_phase0_send\t0\t1\t6669786564\n",
        }
        for filename, contents in oneway_outputs.items():
            (evidence_dir / filename).write_text(contents)

        def oneway_checked_manifest(java_result):
            changed_scenario = {
                **oneway_scenario,
                "live_status": "PASS",
                "evidence": evidence_rel,
            }
            changed_scenarios = [
                changed_scenario if row["id"] == oneway_scenario["id"] else row
                for row in checked_in["scenarios"]
            ]
            manifest = {**checked_in, "scenarios": changed_scenarios}
            if java_result is not None:
                (evidence_dir / "java-oneway.tsv").write_text(java_result)
            return manifest

        validate_manifest(oneway_checked_manifest(None))
        try:
            validate_manifest(
                oneway_checked_manifest(
                    oneway_outputs["java-oneway.tsv"].replace(
                        "NO_RESPONSE", "SEND_OK", 1
                    )
                )
            )
            raise AssertionError("oneway evidence accepted a Broker response result")
        except ValueError as error:
            assert "Java oneway send did not return without a Broker response" in str(error)
    batch_scenario = next(
        scenario
        for scenario in checked_in["scenarios"]
        if scenario["id"] == "PROD-004-batch-send"
    )
    with tempfile.TemporaryDirectory(prefix="compare-self-test-", dir=evidence_root) as temporary:
        evidence_dir = Path(temporary)
        evidence_rel = str(evidence_dir.resolve())
        (evidence_dir / "metadata.txt").write_text("exit_status=0\n")
        (evidence_dir / "result.txt").write_text(
            "PASS PROD-004-batch-send: synthetic validator evidence\n"
        )
        java_batch_id = f"{1:032x}"
        java_second_batch_id = f"{3:032x}"
        rust_batch_id = f"{2:032x}"
        rust_second_batch_id = f"{4:032x}"
        batch_outputs = {
            "java-batch-send.tsv": (
                f"BATCH_SEND\tSEND_OK\tcompat_phase0_batch\t0\t0\t{java_batch_id},{java_second_batch_id}\t{java_batch_id}\t1\t2\n"
            ),
            "rust-batch-send.tsv": (
                f"BATCH_SEND\tSEND_OK\tcompat_phase0_batch\t0\t2\t{rust_batch_id},{rust_second_batch_id}\t{rust_batch_id}\t2\t2\n"
            ),
            "rust-read-java.tsv": (
                "MESSAGE\tcompat_phase0_batch\t0\t0\t62617463682d30\n"
                "MESSAGE\tcompat_phase0_batch\t0\t1\t62617463682d31\n"
            ),
            "java-read-rust.tsv": (
                "MESSAGE\tcompat_phase0_batch\t0\t2\t62617463682d30\n"
                "MESSAGE\tcompat_phase0_batch\t0\t3\t62617463682d31\n"
            ),
        }
        for filename, contents in batch_outputs.items():
            (evidence_dir / filename).write_text(contents)

        def batch_checked_manifest(java_readback):
            changed_scenario = {
                **batch_scenario,
                "live_status": "PASS",
                "evidence": evidence_rel,
            }
            changed_scenarios = [
                changed_scenario if row["id"] == batch_scenario["id"] else row
                for row in checked_in["scenarios"]
            ]
            if java_readback is not None:
                (evidence_dir / "java-read-rust.tsv").write_text(java_readback)
            return {**checked_in, "scenarios": changed_scenarios}

        validate_manifest(batch_checked_manifest(None))
        try:
            validate_manifest(
                batch_checked_manifest(
                    "MESSAGE\tcompat_phase0_batch\t0\t2\t62617463682d30\n"
                )
            )
            raise AssertionError("batch evidence accepted an omitted batch message")
        except ValueError as error:
            assert "batch readback must contain 2 rows" in str(error)
    phase6 = [case for case in checked_in["scenarios"] if case["id"] == "P6-V04"]
    assert len(phase6) == 1, "Phase 6 live handoff case must retain its original ID"
    assert phase6[0]["phase"] == 6 and phase6[0]["execution_phase"] == 19
    assert phase6[0]["live_status"] in {"PENDING_PHASE_19", "PASS", "PASS_RECORDED"}
    assert phase6[0]["inputs"]["checks"] == [
        "heartbeat-subscription-change", "unregister", "remaining-shared-user-functional"]
    missing_handoff = {
        **checked_in,
        "scenarios": [case for case in checked_in["scenarios"] if case["id"] != "P11-V04"],
    }
    try:
        validate_manifest(missing_handoff)
        raise AssertionError("manifest accepted an omitted Phase 11 live handoff")
    except ValueError as error:
        assert "P11-V04" in str(error)
    missing_late_handoff = {
        **checked_in,
        "scenarios": [case for case in checked_in["scenarios"] if case["id"] != "P17-V02"],
    }
    try:
        validate_manifest(missing_late_handoff)
        raise AssertionError("manifest accepted an omitted Phase 17 live handoff")
    except ValueError as error:
        assert "P17-V02" in str(error)
    missing_assertion = {
        **checked_in,
        "scenarios": [
            {**case, "assertions": []} if case["id"] == "P17-V02" else case
            for case in checked_in["scenarios"]
        ],
    }
    try:
        validate_manifest(missing_assertion)
        raise AssertionError("manifest accepted a handoff without assertions")
    except ValueError as error:
        assert "P17-V02" in str(error)
    missing_rust_direction = {
        **checked_in,
        "scenarios": [
            {
                **case,
                "client_directions": ["java-5.5.1 client -> server-5.3.4"],
            }
            if case["id"] == "P17-V02"
            else case
            for case in checked_in["scenarios"]
        ],
    }
    try:
        validate_manifest(missing_rust_direction)
        raise AssertionError("manifest accepted a handoff without a Rust direction")
    except ValueError as error:
        assert "P17-V02" in str(error)
    wrong_source_phase = {
        **checked_in,
        "scenarios": [
            {**case, "phase": 10} if case["id"] == "P11-V04" else case
            for case in checked_in["scenarios"]
        ],
    }
    try:
        validate_manifest(wrong_source_phase)
        raise AssertionError("manifest accepted a Phase 11 case assigned to Phase 10")
    except ValueError as error:
        assert "P11-V04" in str(error)
    print("Differential comparator and manifest self-test passed")


def main(argv):
    if argv == ["--self-test"]:
        self_test()
        return 0
    if argv == ["--validate-manifest"]:
        count = validate_manifest(json.loads(configured_manifest_path().read_text()))
        print(f"Validated {count} scenarios")
        return 0
    if len(argv) != 2:
        raise SystemExit("usage: compare.py --self-test | --validate-manifest | LEFT.json RIGHT.json")
    left, right = (json.loads(Path(name).read_text()) for name in argv)
    if compare(left, right):
        print("match")
        return 0
    print("results differ", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
