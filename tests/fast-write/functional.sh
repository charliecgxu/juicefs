#!/usr/bin/env bash

set -euo pipefail

fast_write_bin=${FAST_WRITE_BIN:?FAST_WRITE_BIN is required}
community_bin=${COMMUNITY_BIN:?COMMUNITY_BIN is required}
test_base=${TEST_BASE:-/data}
keep_results=${KEEP_RESULTS:-1}
dirty_ratio=${FUSE_MAX_DIRTY_RATIO:-50}
sync_concurrency=${FAST_WRITE_SYNC_CONCURRENCY:-16}

for command in mountpoint findmnt python3 sha256sum cmp fallocate; do
	if ! command -v "$command" >/dev/null 2>&1; then
		echo "Required command is missing: $command" >&2
		exit 1
	fi
done
for binary in "$fast_write_bin" "$community_bin"; do
	if [[ ! -x "$binary" ]]; then
		echo "JuiceFS binary is not executable: $binary" >&2
		exit 1
	fi
done
if [[ $(uname -s) != Linux || ! -e /dev/fuse ]]; then
	echo "Fast Write functional tests require Linux and /dev/fuse" >&2
	exit 1
fi
if ((EUID != 0)); then
	echo "Fast Write functional tests must run as root" >&2
	exit 1
fi
if [[ ! -d "$test_base" ]]; then
	echo "TEST_BASE does not exist: $test_base" >&2
	exit 1
fi

root=$(mktemp -d "${test_base%/}/fast-write-functional.XXXXXX")
mnt="$root/mnt"
object_dir="$root/objects"
cache_dir="$root/cache"
community_cache="$root/community-cache"
fast_write_dir="$cache_dir/fastwrite"
logs="$root/logs"
meta_url="sqlite3://$root/meta.db"
mount_pid=
mount_count=0

cleanup_mount() {
	if mountpoint -q "$mnt" 2>/dev/null; then
		umount "$mnt" 2>/dev/null || fusermount -uz "$mnt" 2>/dev/null || umount -l "$mnt" 2>/dev/null || true
	fi
	if [[ -n "${mount_pid:-}" ]]; then
		kill "$mount_pid" 2>/dev/null || true
		wait "$mount_pid" 2>/dev/null || true
		mount_pid=
	fi
}

cleanup() {
	cleanup_mount
	if [[ "$keep_results" == 0 && "$root" == "$test_base"/fast-write-functional.* ]]; then
		rm -rf -- "$root"
	fi
}
trap cleanup EXIT

mkdir -p "$mnt" "$object_dir" "$cache_dir" "$community_cache" "$logs"

"$fast_write_bin" format \
	--trash-days 0 \
	--storage file \
	--bucket "$object_dir" \
	"$meta_url" fast-write-functional >"$logs/format.log" 2>&1

wait_for_mount() {
	for _ in $(seq 1 200); do
		if mountpoint -q "$mnt"; then
			return 0
		fi
		if ! kill -0 "$mount_pid" 2>/dev/null; then
			echo "Mount process exited before $mnt became ready" >&2
			return 1
		fi
		sleep 0.05
	done
	echo "Timed out waiting for mount $mnt" >&2
	return 1
}

start_fast_write() {
	local writeback=${1:-0}
	mount_count=$((mount_count + 1))
	local args=(
		mount
		--no-usage-report
		--log "$logs/fast-write-mount-$mount_count.log"
		--cache-dir "$cache_dir"
		--cache-size 0
		--fast-write-dir "$fast_write_dir"
		--fast-write-sync-concurrency "$sync_concurrency"
		--fuse-max-dirty-ratio "$dirty_ratio"
	)
	if [[ "$writeback" == 1 ]]; then
		args+=(--writeback)
	fi
	"$fast_write_bin" "${args[@]}" "$meta_url" "$mnt" \
		>"$logs/fast-write-mount-$mount_count.stdout" 2>&1 &
	mount_pid=$!
	wait_for_mount
}

start_community() {
	mount_count=$((mount_count + 1))
	"$community_bin" mount \
		--no-usage-report \
		--log "$logs/community-mount-$mount_count.log" \
		--cache-dir "$community_cache" \
		--cache-size 0 \
		--atime-mode noatime \
		-o writeback_cache \
		"$meta_url" "$mnt" >"$logs/community-mount-$mount_count.stdout" 2>&1 &
	mount_pid=$!
	wait_for_mount
}

stop_mount() {
	if mountpoint -q "$mnt"; then
		umount "$mnt"
	fi
	if [[ -n "$mount_pid" ]]; then
		wait "$mount_pid" 2>/dev/null || true
		mount_pid=
	fi
}

crash_mount() {
	local pid=$mount_pid
	mount_pid=
	kill -9 "$pid"
	wait "$pid" 2>/dev/null || true
	for _ in $(seq 1 100); do
		if ! mountpoint -q "$mnt"; then
			return 0
		fi
		sleep 0.05
	done
	fusermount -uz "$mnt" 2>/dev/null || umount -l "$mnt"
}

check_fast_write_dirty_ratio() {
	local device path actual
	device=$(findmnt -n -o MAJ:MIN --target "$mnt" | tr -d '[:space:]')
	path="/sys/class/bdi/$device/max_ratio"
	if [[ ! -r "$path" ]]; then
		echo "FUSE BDI max_ratio is not readable: $path" >&2
		return 1
	fi
	actual=$(tr -d '[:space:]' <"$path")
	if [[ "$actual" != "$dirty_ratio" ]]; then
		echo "FUSE BDI max_ratio is $actual, expected $dirty_ratio" >&2
		return 1
	fi
	printf 'device=%s\nmax_ratio=%s\n' "$device" "$actual" >"$root/fast-write-bdi.txt"
}

start_fast_write 0
check_fast_write_dirty_ratio
mkdir "$mnt/published"

python3 - "$mnt/published/image.bin" "$root/image.expected" <<'PY'
import os
import sys

actual, expected = sys.argv[1:]
chunk = 64 * 1024 * 1024
length = chunk + 3 * 4096
operations = [
    (0, bytes([0x11]) * 4096),
    (123, bytes((i * 17 + 3) % 251 for i in range(777))),
    (4095, bytes((i * 29 + 7) % 253 for i in range(8193))),
    (chunk - 2048, bytes((i * 31 + 11) % 255 for i in range(8192))),
]
for path in (actual, expected):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    try:
        os.ftruncate(fd, length)
        for offset, data in operations:
            assert os.pwrite(fd, data, offset) == len(data)
        os.fsync(fd)
    finally:
        os.close(fd)
PY
cmp "$mnt/published/image.bin" "$root/image.expected"

original_size=$(stat -c %s "$mnt/published/image.bin")
if truncate -s 4096 "$mnt/published/image.bin" >"$logs/shrink.stdout" 2>"$logs/shrink.stderr"; then
	echo "Fast Write unexpectedly allowed shrinking truncate" >&2
	exit 1
fi
[[ $(stat -c %s "$mnt/published/image.bin") == "$original_size" ]]

if fallocate --punch-hole --keep-size --offset 16384 --length 4096 "$mnt/published/image.bin" \
	>"$logs/punch-hole.stdout" 2>"$logs/punch-hole.stderr"; then
	python3 - "$root/image.expected" <<'PY'
import os
import sys
fd = os.open(sys.argv[1], os.O_RDWR)
try:
    assert os.pwrite(fd, bytes(4096), 16384) == 4096
finally:
    os.close(fd)
PY
	echo supported >"$root/punch-hole.result"
else
	python3 - "$mnt/published/image.bin" "$root/image.expected" <<'PY'
import os
import sys
for path in sys.argv[1:]:
    fd = os.open(path, os.O_RDWR)
    try:
        assert os.pwrite(fd, bytes(4096), 16384) == 4096
        os.fsync(fd)
    finally:
        os.close(fd)
PY
	echo kernel-fallback-to-write-zero >"$root/punch-hole.result"
fi

if fallocate --zero-range --keep-size --offset 24583 --length 913 "$mnt/published/image.bin" \
	>"$logs/zero-range.stdout" 2>"$logs/zero-range.stderr"; then
	zero_paths=("$root/image.expected")
	echo supported >"$root/zero-range.result"
else
	zero_paths=("$mnt/published/image.bin" "$root/image.expected")
	echo kernel-fallback-to-write-zero >"$root/zero-range.result"
fi
python3 - "${zero_paths[@]}" <<'PY'
import os
import sys
for path in sys.argv[1:]:
    fd = os.open(path, os.O_RDWR)
    try:
        assert os.pwrite(fd, bytes(913), 24583) == 913
        os.fsync(fd)
    finally:
        os.close(fd)
PY

cp --sparse=always "$mnt/published/image.bin" "$mnt/published/copy.bin"
python3 - "$mnt/published/copy.bin" <<'PY'
import os
import sys
fd = os.open(sys.argv[1], os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
cmp "$mnt/published/copy.bin" "$root/image.expected"
ln "$mnt/published/copy.bin" "$mnt/published/copy.link"
mv "$mnt/published/copy.link" "$mnt/published/copy.renamed"
rm "$mnt/published/copy.bin"
cmp "$mnt/published/copy.renamed" "$root/image.expected"

"$fast_write_bin" upload-fast-write --wait "$mnt/published" >"$logs/upload-published.log"
cmp "$mnt/published/image.bin" "$root/image.expected"
cmp "$mnt/published/copy.renamed" "$root/image.expected"
stop_mount

start_community
cmp "$mnt/published/image.bin" "$root/image.expected"
cmp "$mnt/published/copy.renamed" "$root/image.expected"
stop_mount

start_fast_write 0
python3 - "$mnt/recovery.bin" "$root/recovery.expected" <<'PY'
import os
import sys
data = bytes((i * 43 + 19) % 251 for i in range(4 * 1024 * 1024 + 333))
for path in sys.argv[1:]:
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    try:
        assert os.write(fd, data) == len(data)
        os.fsync(fd)
    finally:
        os.close(fd)
PY
crash_mount

start_fast_write 0
cmp "$mnt/recovery.bin" "$root/recovery.expected"
"$fast_write_bin" upload-fast-write --wait "$mnt/recovery.bin" >"$logs/upload-recovery.log"

python3 - "$mnt/async.bin" "$root/async.expected" <<'PY'
import os
import sys
data = bytes((i * 47 + 23) % 253 for i in range(2 * 1024 * 1024 + 777))
for path in sys.argv[1:]:
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    try:
        assert os.write(fd, data) == len(data)
        os.fsync(fd)
    finally:
        os.close(fd)
PY
"$fast_write_bin" upload-fast-write "$mnt/async.bin" >"$logs/upload-async.log"
python3 - "$mnt/async.bin" "$root/async.expected" <<'PY'
import os
import sys
patch = bytes((i * 53 + 29) % 255 for i in range(8192))
for path in sys.argv[1:]:
    fd = os.open(path, os.O_RDWR)
    try:
        assert os.pwrite(fd, patch, 4093) == len(patch)
        os.fsync(fd)
    finally:
        os.close(fd)
PY
"$fast_write_bin" upload-fast-write --wait "$mnt/async.bin" >"$logs/upload-async-wait.log"
cmp "$mnt/async.bin" "$root/async.expected"
stop_mount

start_community
cmp "$mnt/recovery.bin" "$root/recovery.expected"
cmp "$mnt/async.bin" "$root/async.expected"
stop_mount

start_fast_write 1
python3 - "$mnt/staging.bin" "$root/staging.expected" <<'PY'
import os
import sys
data = bytes((i * 59 + 31) % 251 for i in range(8 * 1024 * 1024 + 123))
for path in sys.argv[1:]:
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    try:
        assert os.write(fd, data) == len(data)
        os.fsync(fd)
    finally:
        os.close(fd)
PY
"$fast_write_bin" upload-fast-write --wait "$mnt/staging.bin" >"$logs/upload-staging.log"
cmp "$mnt/staging.bin" "$root/staging.expected"
stop_mount

start_community
cmp "$mnt/staging.bin" "$root/staging.expected"
stop_mount

start_fast_write 1
mount_log="$logs/fast-write-mount-$mount_count.log"
log_bytes=$(wc -c <"$mount_log")
set +e
python3 - "$mnt" "$root/tmpfile.expected" <<'PY'
import ctypes
import os
import sys

directory, output = sys.argv[1:]
data = bytes((i * 17 + 3) % 251 for i in range(4096 + 100))
try:
    fd = os.open(directory, os.O_TMPFILE | os.O_RDWR, 0o644)
except OSError as exc:
    if exc.errno == 95:
        sys.exit(95)
    raise
try:
    assert os.write(fd, data) == len(data)
    os.fsync(fd)
    libc = ctypes.CDLL(None, use_errno=True)
    path = os.fsencode(os.path.join(directory, "tmpfile.bin"))
    if libc.linkat(fd, b"", -100, path, 0x1000) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
finally:
    os.close(fd)
with open(output, "wb") as expected:
    expected.write(data)
PY
tmpfile_status=$?
set -e
if [[ "$tmpfile_status" -eq 95 ]]; then
	if tail -c +"$((log_bytes + 1))" "$mount_log" | grep -q "create"; then
		echo "O_TMPFILE reached JuiceFS and was rejected" >&2
		exit 1
	fi
	echo "Kernel does not implement FUSE O_TMPFILE; skipped"
	stop_mount
elif [[ "$tmpfile_status" -ne 0 ]]; then
	exit "$tmpfile_status"
else
	"$fast_write_bin" upload-fast-write --wait "$mnt/tmpfile.bin" >"$logs/upload-tmpfile.log"
	cmp "$mnt/tmpfile.bin" "$root/tmpfile.expected"
	stop_mount

	start_community
	cmp "$mnt/tmpfile.bin" "$root/tmpfile.expected"
	stop_mount
fi

{
	uname -a
	"$fast_write_bin" version
	"$community_bin" version
	findmnt -n -o SOURCE,FSTYPE,OPTIONS --target "$test_base"
} >"$root/environment.txt"
sha256sum "$root"/*.expected >"$root/checksums.txt"

echo "Fast Write Linux/FUSE functional suite passed"
echo "Results: $root"
