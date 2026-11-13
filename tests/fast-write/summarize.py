#!/usr/bin/env python3

import argparse
import csv
import json
import statistics
from collections import defaultdict


def write_metrics(path):
    with open(path) as stream:
        document = json.load(stream)
    write = document["jobs"][0]["write"]
    bw_bytes = write.get("bw_bytes")
    if bw_bytes is None:
        bw_bytes = float(write.get("bw", 0)) * 1024
    latency = write.get("clat_ns")
    if latency is None:
        latency = write.get("clat", {})
    percentile = latency.get("percentile", {})
    p99 = percentile.get("99.000000", percentile.get("99.00", 0))
    return {
        "iops": float(write.get("iops", 0)),
        "bw_mib_s": float(bw_bytes) / 1024 / 1024,
        "clat_mean_us": float(latency.get("mean", 0)) / 1000,
        "clat_p99_us": float(p99) / 1000,
        "io_bytes": int(write.get("io_bytes", 0)),
        "total_ios": int(write.get("total_ios", 0)),
    }


def median(samples, key):
    return statistics.median(sample[key] for sample in samples)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("metrics")
    parser.add_argument("summary_json")
    parser.add_argument("summary_tsv")
    parser.add_argument("--upload-ns")
    parser.add_argument("--min-xfs-percent", type=float, default=0)
    parser.add_argument("--min-community-multiplier", type=float, default=0)
    args = parser.parse_args()

    groups = defaultdict(list)
    with open(args.metrics, newline="") as stream:
        for row in csv.DictReader(stream, delimiter="\t"):
            fio = write_metrics(row["json"])
            sample = {
                "round": int(row["round"]),
                "iops": fio["iops"],
                "bw_mib_s": fio["bw_mib_s"],
                "clat_mean_us": fio["clat_mean_us"],
                "clat_p99_us": fio["clat_p99_us"],
                "io_bytes": fio["io_bytes"],
                "total_ios": fio["total_ios"],
                "fio_wall_s": int(row["fio_wall_ns"]) / 1e9,
                "fsync_wall_s": int(row["fsync_wall_ns"]) / 1e9,
                "total_wall_s": int(row["total_wall_ns"]) / 1e9,
            }
            sample["effective_iops"] = (
                sample["total_ios"] / sample["fio_wall_s"]
                if sample["fio_wall_s"]
                else 0
            )
            sample["effective_bw_mib_s"] = (
                sample["io_bytes"] / 1024 / 1024 / sample["fio_wall_s"]
                if sample["fio_wall_s"]
                else 0
            )
            groups[(row["target"], row["workload"])].append(sample)

    summary = {"groups": {}, "comparisons": {}}
    for (target, workload), samples in sorted(groups.items()):
        values = {
            "runs": samples,
            "median_iops": median(samples, "iops"),
            "median_bw_mib_s": median(samples, "bw_mib_s"),
            "median_effective_iops": median(samples, "effective_iops"),
            "median_effective_bw_mib_s": median(samples, "effective_bw_mib_s"),
            "median_clat_mean_us": median(samples, "clat_mean_us"),
            "median_clat_p99_us": median(samples, "clat_p99_us"),
            "median_fio_wall_s": median(samples, "fio_wall_s"),
            "median_fsync_wall_s": median(samples, "fsync_wall_s"),
            "median_total_wall_s": median(samples, "total_wall_s"),
        }
        summary["groups"].setdefault(target, {})[workload] = values

    workloads = sorted(summary["groups"].get("xfs", {}))
    for workload in workloads:
        xfs = summary["groups"]["xfs"][workload]
        comparison = {}
        for target in ("community", "fast-write"):
            if workload not in summary["groups"].get(target, {}):
                continue
            values = summary["groups"][target][workload]
            comparison[target] = {
                "iops_xfs_percent": (
                    values["median_iops"] / xfs["median_iops"] * 100
                    if xfs["median_iops"]
                    else 0
                ),
                "effective_iops_xfs_percent": (
                    values["median_effective_iops"]
                    / xfs["median_effective_iops"]
                    * 100
                    if xfs["median_effective_iops"]
                    else 0
                ),
                "bw_xfs_percent": (
                    values["median_bw_mib_s"] / xfs["median_bw_mib_s"] * 100
                    if xfs["median_bw_mib_s"]
                    else 0
                ),
                "inverse_total_time_xfs_percent": (
                    xfs["median_total_wall_s"] / values["median_total_wall_s"] * 100
                    if values["median_total_wall_s"]
                    else 0
                ),
            }
        community = summary["groups"].get("community", {}).get(workload)
        fast_write = summary["groups"].get("fast-write", {}).get(workload)
        if community and fast_write:
            comparison["fast_write_vs_community"] = {
                "iops_multiplier": (
                    fast_write["median_iops"] / community["median_iops"]
                    if community["median_iops"]
                    else 0
                ),
                "effective_iops_multiplier": (
                    fast_write["median_effective_iops"]
                    / community["median_effective_iops"]
                    if community["median_effective_iops"]
                    else 0
                ),
                "bw_multiplier": (
                    fast_write["median_bw_mib_s"] / community["median_bw_mib_s"]
                    if community["median_bw_mib_s"]
                    else 0
                ),
                "inverse_total_time_multiplier": (
                    community["median_total_wall_s"] / fast_write["median_total_wall_s"]
                    if fast_write["median_total_wall_s"]
                    else 0
                ),
            }
        summary["comparisons"][workload] = comparison

    if args.upload_ns:
        with open(args.upload_ns) as stream:
            summary["fast_write_upload_wait_s"] = int(stream.read().strip()) / 1e9

    with open(args.summary_json, "w") as stream:
        json.dump(summary, stream, indent=2, sort_keys=True)
        stream.write("\n")

    with open(args.summary_tsv, "w", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t")
        writer.writerow(
            [
                "target",
                "workload",
                "fio_iops",
                "effective_iops",
                "fio_mib_s",
                "effective_mib_s",
                "median_p99_us",
                "median_fio_s",
                "median_fsync_s",
                "median_total_s",
                "effective_xfs_percent",
                "effective_community_multiplier",
            ]
        )
        for target in ("xfs", "community", "fast-write"):
            for workload, values in sorted(summary["groups"].get(target, {}).items()):
                xfs_percent = ""
                community_multiplier = ""
                comparison = summary["comparisons"].get(workload, {})
                if target != "xfs":
                    xfs_percent = comparison.get(target, {}).get(
                        "effective_iops_xfs_percent", ""
                    )
                if target == "fast-write":
                    community_multiplier = comparison.get(
                        "fast_write_vs_community", {}
                    ).get("effective_iops_multiplier", "")
                writer.writerow(
                    [
                        target,
                        workload,
                        "%.2f" % values["median_iops"],
                        "%.2f" % values["median_effective_iops"],
                        "%.2f" % values["median_bw_mib_s"],
                        "%.2f" % values["median_effective_bw_mib_s"],
                        "%.2f" % values["median_clat_p99_us"],
                        "%.4f" % values["median_fio_wall_s"],
                        "%.4f" % values["median_fsync_wall_s"],
                        "%.4f" % values["median_total_wall_s"],
                        "%.2f" % xfs_percent if xfs_percent != "" else "",
                        (
                            "%.2f" % community_multiplier
                            if community_multiplier != ""
                            else ""
                        ),
                    ]
                )

    gate = summary["comparisons"].get("buffered-rand-q16", {})
    fast_xfs = gate.get("fast-write", {}).get("effective_iops_xfs_percent", 0)
    multiplier = gate.get("fast_write_vs_community", {}).get(
        "effective_iops_multiplier", 0
    )
    failures = []
    if args.min_xfs_percent and fast_xfs < args.min_xfs_percent:
        failures.append(
            "Fast Write is %.2f%% of XFS, below %.2f%%"
            % (fast_xfs, args.min_xfs_percent)
        )
    if (
        args.min_community_multiplier
        and multiplier < args.min_community_multiplier
    ):
        failures.append(
            "Fast Write is %.2fx community, below %.2fx"
            % (multiplier, args.min_community_multiplier)
        )
    if failures:
        raise SystemExit("; ".join(failures))


if __name__ == "__main__":
    main()
