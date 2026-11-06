#!/usr/bin/env python3
"""Check workspace metadata, crate license files, and dependency direction."""

import json
import subprocess
from pathlib import Path

workspace_root = Path(__file__).resolve().parents[1]
license_documents = {
    name: (workspace_root / name).read_bytes() for name in ("LICENSE", "NOTICE")
}

metadata = json.loads(
    subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"], text=True
    )
)
packages = {package["name"]: package for package in metadata["packages"]}
expected = {
    "rocketmq-classic-common",
    "rocketmq-classic-consumer",
    "rocketmq-classic-core",
    "rocketmq-classic",
    "rocketmq-classic-protocol",
    "rocketmq-classic-producer",
    "rocketmq-classic-remoting",
    "rocketmq-classic-integration",
}
product_packages = expected - {"rocketmq-classic-integration"}
assert set(packages) == expected, f"unexpected workspace packages: {set(packages)}"

common_deps = {dependency["name"] for dependency in packages["rocketmq-classic-common"]["dependencies"]}
protocol_deps = {dependency["name"] for dependency in packages["rocketmq-classic-protocol"]["dependencies"]}
remoting_deps = {dependency["name"] for dependency in packages["rocketmq-classic-remoting"]["dependencies"]}
assert "rocketmq-classic-protocol" not in common_deps, "common must not depend on protocol"
assert "rocketmq-classic-common" in protocol_deps, "protocol must depend on common"
assert "rocketmq-classic-remoting" not in protocol_deps, "protocol must not depend on remoting"
assert "rocketmq-classic-protocol" in remoting_deps, "remoting must depend on protocol"
assert all(package["publish"] == [] for package in packages.values())
for package in packages.values():
    assert package["license"] == "Apache-2.0", f"unexpected license for {package['name']}"
    if package["name"] in product_packages:
        package_root = Path(package["manifest_path"]).parent
        for name, expected_content in license_documents.items():
            packaged_document = package_root / name
            assert packaged_document.is_file(), f"{package['name']} package is missing {name}"
            assert packaged_document.read_bytes() == expected_content, (
                f"{package['name']} {name} differs from the workspace copy"
            )
    else:
        assert package["publish"] == [], "integration runner must stay unpublished"
print("workspace metadata, package license files, and dependency direction PASS")
