#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$script_dir" && git rev-parse --show-toplevel)
go_bin=${GO_BIN:-go}
out_dir=${OUT_DIR:-/tmp/juicefs-fast-write-test-bin}
run_fuse=${RUN_FUSE:-1}
run_performance=${RUN_PERFORMANCE:-1}

GO_BIN="$go_bin" OUT_DIR="$out_dir" \
	bash "$script_dir/build-binaries.sh"
# shellcheck disable=SC1090
source "$out_dir/binaries.env"

cd "$repo"
export MOCKEY_CHECK_GCFLAGS=false

"$go_bin" test ./pkg/vfs -run 'FastWrite' -count=1 -timeout=10m
"$go_bin" test -race ./pkg/vfs -run 'FastWrite' -count=1 -timeout=15m
"$go_bin" test ./pkg/vfs \
	-skip '^(TestReaddirCache|TestVFSReadDirSort|TestReadDirBatch|TestReaddir)$' \
	-count=1 -timeout=15m
"$go_bin" test ./pkg/chunk -run '^TestFlushStagingSlice' -count=1
"$go_bin" test ./pkg/fuse -run '^TestSetMaxDirtyRatioAt' -count=1
"$go_bin" test ./cmd \
	-run '^(TestFuseMaxDirtyRatioFlag|TestFastWriteMountFlags|TestValidateFastWriteMountPath|TestValidateFastWriteMountConfig|TestApplyFastWriteMountDependencies|TestUploadFastWriteCommand|TestEncodeUploadFastWriteRequest)$' \
	-count=1
"$go_bin" test ./pkg/meta \
	-run '^(TestMemKVClient|TestSQLiteClient|TestFastWriteControlCommandID)$' \
	-count=1 -timeout=10m
"$go_bin" test ./pkg/vfs -run '^$' -bench 'FastWrite' -benchtime=100x -count=1

if [[ "$run_fuse" == 1 ]]; then
	FAST_WRITE_BIN="$FAST_WRITE_BIN" \
		COMMUNITY_BIN="$COMMUNITY_BIN" \
		bash "$script_dir/functional.sh"
fi

if [[ "$run_performance" == 1 ]]; then
	FAST_WRITE_BIN="$FAST_WRITE_BIN" \
		COMMUNITY_BIN="$COMMUNITY_BIN" \
		bash "$script_dir/performance.sh"
fi
