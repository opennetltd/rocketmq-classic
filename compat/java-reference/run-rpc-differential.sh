#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
oracle="$PWD/fixtures/rpc"
rust="$PWD/target/p5-rust-rpc-frames"
mvn -q -f compat/java-reference/pom.xml compile
mvn -q -f compat/java-reference/pom.xml exec:java -Dexec.args="rpc-fixtures --output $oracle"
mkdir -p "$rust"
P5_RUST_FRAMES="$rust" cargo test -p rocketmq-classic-core --test rpc_java_fixtures --locked
mvn -q -f compat/java-reference/pom.xml exec:java -Dexec.args="verify-rpc-frames --dir $rust --oracle $oracle"