#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="${ROCKETMQ_P19_CACHE_DIR:-${TMPDIR:-/tmp}/rocketmq-classic-p19-cache}"
archive_name=rocketmq-all-5.3.4-bin-release.zip
archive_url="https://downloads.apache.org/rocketmq/5.3.4/$archive_name"
checksum_url="$archive_url.sha512"
expected_sha512=88508d5480421ce1582e56aa2e3b6e28ea6b88418cf618fd79a78f996a7ae853ee1ea98780377235ed396ce2bc82b4f3d88a838c72076760466702d881bfc6ae
image_tag=rocketmq-classic-p19-official-5.3.4-amd64:88508d548042

mkdir -p "$cache_dir"
archive_path="$cache_dir/$archive_name"
checksum_path="$archive_path.sha512"
if [[ ! -s "$archive_path" ]]; then
    curl --fail --location --silent --show-error --retry 3 --output "$archive_path" "$archive_url"
fi
if [[ ! -s "$checksum_path" ]]; then
    curl --fail --location --silent --show-error --retry 3 --output "$checksum_path" "$checksum_url"
fi
python3 - "$archive_path" "$checksum_path" "$expected_sha512" <<'PY'
import hashlib
import re
import sys
from pathlib import Path

archive = Path(sys.argv[1])
checksum_file = Path(sys.argv[2])
expected = sys.argv[3].lower()
checksum_body = checksum_file.read_text().split(":", 1)[-1]
published = "".join(re.findall(r"[0-9a-fA-F]{8}", checksum_body)).lower()
actual = hashlib.sha512(archive.read_bytes()).hexdigest()
print(f"official_distribution={archive.name}", file=sys.stderr)
print(f"published_sha512={published}", file=sys.stderr)
print(f"actual_sha512={actual}", file=sys.stderr)
if published != expected or actual != expected:
    raise SystemExit("Apache RocketMQ 5.3.4 binary distribution SHA-512 verification failed")
PY

context_dir="$(mktemp -d "${TMPDIR:-/tmp}/rocketmq-classic-p19-5.3.4-amd64.XXXXXX")"
trap 'rm -rf "$context_dir"' EXIT
unzip -q "$archive_path" -d "$context_dir"
cp "$repo_root/integration-tests/docker/rocketmq-5.3.4-entrypoint.sh" "$context_dir/docker-entrypoint.sh"
docker buildx build --platform linux/amd64 --pull --no-cache --load \
    --tag "$image_tag" \
    --file "$repo_root/integration-tests/docker/rocketmq-5.3.4-amd64.Dockerfile" \
    "$context_dir" >&2

platform="$(docker image inspect "$image_tag" --format '{{.Os}}/{{.Architecture}}')"
if [[ "$platform" != "linux/amd64" ]]; then
    echo "Verified RocketMQ distribution image has unexpected platform '$platform'" >&2
    exit 1
fi
printf '%s\n' "$image_tag"
