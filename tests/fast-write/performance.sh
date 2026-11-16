#!/usr/bin/env bash

set -euo pipefail

fast_write_bin=${FAST_WRITE_BIN:?FAST_WRITE_BIN is required}
community_bin=${COMMUNITY_BIN:?COMMUNITY_BIN is required}
test_base=${TEST_BASE:-/data}
profile=${PROFILE:-full}
rounds=${ROUNDS:-}
file_size=${FILE_SIZE:-4G}
dirty_ratio=${FUSE_MAX_DIRTY_RATIO:-50}
sync_concurrency=${FAST_WRITE_SYNC_CONCURRENCY:-16}
keep_results=${KEEP_RESULTS:-1}
juicefs_writeback=${JUICEFS_WRITEBACK:-1}
min_xfs_percent=${FAST_WRITE_MIN_XFS_PERCENT:-0}
min_community_multiplier=${FAST_WRITE_MIN_COMMUNITY_MULTIPLIER:-0}
default_workloads=buffered-rand-single,buffered-rand-q16,buffered-rand-q128,buffered-rand-q16-fsync,direct-rand-q16,buffered-seq-single
workloads=${WORKLOADS:-$default_workloads}
targets=${TARGETS:-xfs,community,fast-write}

case "$profile" in
smoke)
	rounds=${rounds:-1}
	single_io_size=${TOTAL_IO_SIZE:-32M}
	per_job_io_size=${PER_JOB_IO_SIZE:-2M}
	upload_size=${UPLOAD_SIZE:-64M}
	;;
full)
	rounds=${rounds:-3}
	single_io_size=${TOTAL_IO_SIZE:-500M}
	per_job_io_size=${PER_JOB_IO_SIZE:-32000k}
	upload_size=${UPLOAD_SIZE:-512M}
	;;
*)
	echo "PROFILE must be smoke or full, got $profile" >&2
	exit 1
	;;
esac

for command in fio python3 mountpoint findmnt truncate; do
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
if [[ $(uname -s) != Linux || ! -e /dev/fuse || $EUID -ne 0 ]]; then
	echo "Performance tests require Linux, /dev/fuse, and root" >&2
	exit 1
fi
if [[ ! -d "$test_base" ]]; then
	echo "TEST_BASE does not exist: $test_base" >&2
	exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$script_dir" && git rev-parse --show-toplevel)
root=$(mktemp -d "${test_base%/}/fast-write-performance.XXXXXX")
results="$root/results"
fio_results="$results/fio"
xfs_dir="$root/xfs"
community_root="$root/community"
fast_root="$root/fast-write"
community_mnt="$community_root/mnt"
fast_mnt="$fast_root/mnt"
community_meta="sqlite3://$community_root/meta.db"
fast_meta="sqlite3://$fast_root/meta.db"
metrics="$results/metrics.tsv"
community_pid=
fast_pid=

cleanup_mount() {
	local mountpoint_path=$1
	local pid=$2
	if mountpoint -q "$mountpoint_path" 2>/dev/null; then
		umount "$mountpoint_path" 2>/dev/null ||
			fusermount -uz "$mountpoint_path" 2>/dev/null ||
			umount -l "$mountpoint_path" 2>/dev/null || true
	fi
	if [[ -n "$pid" ]]; then
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
}

cleanup() {
	cleanup_mount "$community_mnt" "$community_pid"
	cleanup_mount "$fast_mnt" "$fast_pid"
	if [[ "$keep_results" == 0 && "$root" == "$test_base"/fast-write-performance.* ]]; then
		rm -rf -- "$root"
	fi
}
trap cleanup EXIT

mkdir -p \
	"$results" "$fio_results" "$xfs_dir" \
	"$community_mnt" "$community_root/objects" "$community_root/cache" \
	"$fast_mnt" "$fast_root/objects" "$fast_root/cache"

"$community_bin" format \
	--trash-days 0 --storage file --bucket "$community_root/objects" \
	"$community_meta" community-performance >"$community_root/format.log" 2>&1
"$fast_write_bin" format \
	--trash-days 0 --storage file --bucket "$fast_root/objects" \
	"$fast_meta" fast-write-performance >"$fast_root/format.log" 2>&1

wait_for_mount() {
	local mountpoint_path=$1
	local pid=$2
	for _ in $(seq 1 200); do
		if mountpoint -q "$mountpoint_path"; then
			return 0
		fi
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "Mount process for $mountpoint_path exited early" >&2
			return 1
		fi
		sleep 0.05
	done
	echo "Timed out waiting for $mountpoint_path" >&2
	return 1
}

community_args=(
	mount --no-usage-report --log "$community_root/mount.log"
	--cache-dir "$community_root/cache" --cache-size 0
	--atime-mode noatime -o writeback_cache
)
fast_args=(
	mount --no-usage-report --log "$fast_root/mount.log"
	--cache-dir "$fast_root/cache" --cache-size 0
	--fast-write-dir "$fast_root/cache/fastwrite"
	--fast-write-sync-concurrency "$sync_concurrency"
	--fuse-max-dirty-ratio "$dirty_ratio"
)
if [[ "$juicefs_writeback" == 1 ]]; then
	community_args+=(--writeback)
	fast_args+=(--writeback)
fi

"$community_bin" "${community_args[@]}" "$community_meta" "$community_mnt" \
	>"$community_root/mount.stdout" 2>&1 &
community_pid=$!
wait_for_mount "$community_mnt" "$community_pid"

"$fast_write_bin" "${fast_args[@]}" "$fast_meta" "$fast_mnt" \
	>"$fast_root/mount.stdout" 2>&1 &
fast_pid=$!
wait_for_mount "$fast_mnt" "$fast_pid"

set_bdi_ratio() {
	local mountpoint_path=$1
	local device path actual
	device=$(findmnt -n -o MAJ:MIN --target "$mountpoint_path" | tr -d '[:space:]')
	path="/sys/class/bdi/$device/max_ratio"
	if [[ ! -w "$path" ]]; then
		echo "FUSE BDI max_ratio is not writable: $path" >&2
		return 1
	fi
	printf '%s' "$dirty_ratio" >"$path"
	actual=$(tr -d '[:space:]' <"$path")
	if [[ "$actual" != "$dirty_ratio" ]]; then
		echo "Unable to set $path to $dirty_ratio; read back $actual" >&2
		return 1
	fi
	echo "$device"
}

community_bdi=$(set_bdi_ratio "$community_mnt")
fast_bdi=$(set_bdi_ratio "$fast_mnt")

{
	echo "date=$(date -Iseconds)"
	echo "repository=$repo"
	echo "head=$(cd "$repo" && git rev-parse HEAD)"
	echo "working_tree_changes=$(cd "$repo" && git status --porcelain | wc -l)"
	echo "kernel=$(uname -r)"
	echo "architecture=$(uname -m)"
	echo "cpu=$(awk -F: '/model name/{sub(/^ /, "", $2); print $2; exit}' /proc/cpuinfo)"
	echo "mem_total_kib=$(awk '/^MemTotal:/{print $2}' /proc/meminfo)"
	echo "test_filesystem=$(findmnt -n -o SOURCE,FSTYPE,OPTIONS --target "$test_base")"
	echo "fio=$(fio --version)"
	echo "profile=$profile"
	echo "rounds=$rounds"
	echo "workloads=$workloads"
	echo "targets=$targets"
	echo "file_size=$file_size"
	echo "single_io_size=$single_io_size"
	echo "per_job_io_size=$per_job_io_size"
	echo "upload_size=$upload_size"
	echo "juicefs_writeback=$juicefs_writeback"
	echo "fuse_max_dirty_ratio=$dirty_ratio"
	echo "community_bdi=$community_bdi"
	echo "fast_write_bdi=$fast_bdi"
	echo "vm_dirty_ratio=$(sysctl -n vm.dirty_ratio)"
	echo "vm_dirty_background_ratio=$(sysctl -n vm.dirty_background_ratio)"
	echo "fast_write_binary=$("$fast_write_bin" version 2>&1 | tr '\n' ' ')"
	echo "community_binary=$("$community_bin" version 2>&1 | tr '\n' ' ')"
} >"$results/environment.txt"

printf 'target\tworkload\tround\tfio_wall_ns\tfsync_wall_ns\ttotal_wall_ns\tjson\n' >"$metrics"

target_dir() {
	case "$1" in
	xfs) echo "$xfs_dir" ;;
	community) echo "$community_mnt" ;;
	fast-write) echo "$fast_mnt" ;;
	*) return 1 ;;
	esac
}

prepare_files() {
	local dir=$1
	local prefix=$2
	local jobs=$3
	local index
	for ((index = 0; index < jobs; index++)); do
		truncate -s "$file_size" "$dir/$prefix-$index.bin"
	done
}

fsync_files() {
	local dir=$1
	local prefix=$2
	local jobs=$3
	python3 - "$dir" "$prefix" "$jobs" <<'PY'
import concurrent.futures
import os
import sys

root, prefix, jobs = sys.argv[1], sys.argv[2], int(sys.argv[3])
paths = [os.path.join(root, "%s-%d.bin" % (prefix, index)) for index in range(jobs)]

def fsync_one(path):
    fd = os.open(path, os.O_RDWR)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

with concurrent.futures.ThreadPoolExecutor(max_workers=min(jobs, 16)) as executor:
    list(executor.map(fsync_one, paths))
PY
}

contains_workload() {
	local candidate=$1
	[[ ",$workloads," == *",$candidate,"* ]]
}

contains_target() {
	local candidate=$1
	[[ ",$targets," == *",$candidate,"* ]]
}

run_one() {
	local target=$1
	local workload=$2
	local round=$3
	local dir prefix jobs rw block_size engine depth direct io_size do_fsync
	dir=$(target_dir "$target")
	prefix="${workload}-r${round}"
	jobs=1
	rw=randwrite
	block_size=4k
	engine=sync
	depth=1
	direct=0
	io_size=$single_io_size
	do_fsync=0

	case "$workload" in
	buffered-rand-single)
		;;
	buffered-rand-q16)
		jobs=16
		engine=libaio
		depth=16
		io_size=$per_job_io_size
		;;
	buffered-rand-q128)
		jobs=16
		engine=libaio
		depth=128
		io_size=$per_job_io_size
		;;
	buffered-rand-q16-fsync)
		jobs=16
		engine=libaio
		depth=16
		io_size=$per_job_io_size
		do_fsync=1
		;;
	direct-rand-q16)
		jobs=16
		engine=libaio
		depth=16
		direct=1
		io_size=$per_job_io_size
		;;
	buffered-seq-single)
		rw=write
		block_size=1M
		;;
	*)
		echo "Unknown workload: $workload" >&2
		return 1
		;;
	esac

	prepare_files "$dir" "$prefix" "$jobs"
	local json="$fio_results/${target}-${workload}-r${round}.json"
	local start fio_end fsync_end
	start=$(date +%s%N)
	fio \
		--name="${target}-${workload}-r${round}" \
		--directory="$dir" \
		--filename_format="${prefix}-\$jobnum.bin" \
		--allow_file_create=0 \
		--rw="$rw" \
		--bs="$block_size" \
		--size="$file_size" \
		--io_size="$io_size" \
		--ioengine="$engine" \
		--iodepth="$depth" \
		--numjobs="$jobs" \
		--direct="$direct" \
		--fallocate=none \
		--randrepeat=1 \
		--randseed="$round" \
		--refill_buffers=1 \
		--fsync=0 \
		--fdatasync=0 \
		--end_fsync=0 \
		--group_reporting=1 \
		--output-format=json \
		--output="$json"
	fio_end=$(date +%s%N)
	fsync_end=$fio_end
	if [[ "$do_fsync" == 1 ]]; then
		fsync_files "$dir" "$prefix" "$jobs"
		fsync_end=$(date +%s%N)
	fi
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$target" "$workload" "$round" \
		"$((fio_end - start))" "$((fsync_end - fio_end))" "$((fsync_end - start))" \
		"$json" >>"$metrics"

	if [[ "$do_fsync" == 0 ]]; then
		fsync_files "$dir" "$prefix" "$jobs"
	fi
	rm -f -- "$dir/$prefix"-*.bin
	sync
}

order_for_round() {
	local ordered target selected=()
	case $((($1 - 1) % 3)) in
	0) ordered="xfs community fast-write" ;;
	1) ordered="community fast-write xfs" ;;
	2) ordered="fast-write xfs community" ;;
	esac
	for target in $ordered; do
		if contains_target "$target"; then
			selected+=("$target")
		fi
	done
	echo "${selected[*]}"
}

IFS=',' read -r -a workload_list <<<"$workloads"
IFS=',' read -r -a target_list <<<"$targets"
for target in "${target_list[@]}"; do
	case "$target" in
	xfs|community|fast-write) ;;
	*)
		echo "Unknown target: $target" >&2
		exit 1
		;;
	esac
done
if ((${#target_list[@]} == 0)); then
	echo "TARGETS must select at least one target" >&2
	exit 1
fi
for workload in "${workload_list[@]}"; do
	contains_workload "$workload" || continue
	for round in $(seq 1 "$rounds"); do
		for target in $(order_for_round "$round"); do
			echo "Running target=$target workload=$workload round=$round"
			run_one "$target" "$workload" "$round"
		done
	done
done

upload_file="$fast_mnt/fast-write-upload.bin"
fio \
	--name=fast-write-upload-prepare \
	--filename="$upload_file" \
	--rw=write \
	--bs=1M \
	--size="$upload_size" \
	--io_size="$upload_size" \
	--ioengine=sync \
	--iodepth=1 \
	--numjobs=1 \
	--direct=0 \
	--fallocate=none \
	--end_fsync=1 \
	--output-format=json \
	--output="$fio_results/fast-write-upload-prepare.json"
upload_start=$(date +%s%N)
"$fast_write_bin" upload-fast-write --wait "$upload_file" >"$results/upload-fast-write.txt"
upload_end=$(date +%s%N)
echo "$((upload_end - upload_start))" >"$results/upload-fast-write-wall-ns.txt"
rm -f -- "$upload_file"
sync

python3 "$script_dir/summarize.py" \
	"$metrics" "$results/summary.json" "$results/summary.tsv" \
	--upload-ns "$results/upload-fast-write-wall-ns.txt" \
	--min-xfs-percent "$min_xfs_percent" \
	--min-community-multiplier "$min_community_multiplier"

cat "$results/summary.tsv"
echo "Fast Write performance suite passed"
echo "Results: $root"
