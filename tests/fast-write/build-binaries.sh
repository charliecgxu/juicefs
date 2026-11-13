#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$script_dir" && git rev-parse --show-toplevel)
go_bin=${GO_BIN:-go}
out_dir=${OUT_DIR:-/tmp/juicefs-fast-write-test-bin}
community_commit=${COMMUNITY_COMMIT:-$(cd "$repo" && git rev-parse HEAD^)}

if ! "$go_bin" version >/dev/null 2>&1; then
	echo "Go toolchain is not executable: $go_bin" >&2
	exit 1
fi
if ! (cd "$repo" && git cat-file -e "${community_commit}^{commit}") 2>/dev/null; then
	echo "Community commit does not exist: $community_commit" >&2
	exit 1
fi

mkdir -p "$out_dir"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/juicefs-community-source.XXXXXX")
cleanup() {
	rm -rf -- "$tmp"
}
trap cleanup EXIT

fast_write_bin="$out_dir/juicefs-fast-write"
community_bin="$out_dir/juicefs-community"

echo "Building Fast Write from working tree"
(
	cd "$repo"
	"$go_bin" build -trimpath -o "$fast_write_bin" .
)

echo "Building community baseline $community_commit"
mkdir -p "$tmp/source"
(cd "$repo" && git archive "$community_commit") | tar -x -C "$tmp/source"
(
	cd "$tmp/source"
	"$go_bin" build -trimpath -o "$community_bin" .
)

fast_write_commit=$(cd "$repo" && git rev-parse HEAD)
cat >"$out_dir/binaries.env" <<EOF
FAST_WRITE_BIN=$fast_write_bin
COMMUNITY_BIN=$community_bin
FAST_WRITE_COMMIT=$fast_write_commit
COMMUNITY_COMMIT=$(cd "$repo" && git rev-parse "$community_commit")
EOF

"$fast_write_bin" version
"$community_bin" version
echo "Wrote $out_dir/binaries.env"
