---
title: FUSE Mount Options
sidebar_position: 5
slug: /fuse_mount_options
---

JuiceFS provides several access methods, FUSE is the common one, which is the way to mount the file system locally using the `juicefs mount` command. Users can add FUSE mount options for more granular control.

This guide describes the common FUSE mount options for JuiceFS, with two ways to add mount options:

1. Run [`juicefs mount`](../reference/command_reference.mdx#mount), and use `-o` to specify multiple options separated by commas.

   ```bash
   juicefs mount -d -o allow_other,writeback_cache sqlite3://myjfs.db ~/jfs
   ```

2. When writing `/etc/fstab` items, add FUSE options directly to the `options` field, with multiple options separated by commas.

   ```
   # <file system>       <mount point>   <type>      <options>           <dump>  <pass>
   redis://localhost:6379/1    /jfs      juicefs     _netdev,writeback_cache   0       0
   ```

## default_permissions

This option is automatically enabled when JuiceFS is mounted and does not need to be explicitly specified. It will enable the kernel's file access checks, which are performed outside the filesystem. When enabled, both the kernel checks and the file system checks must succeed before further operations.

:::tip
The kernel performs standard Unix permission checks based on mode bits, UID/GID, and directory entry ownership.
:::

## allow_other

By default FUSE only allows access to the user mounting the file system. `allow_other` option overrides this behavior to allow access for other users. When mounting JuiceFS using root, `allow_other` is automatically assumed (search for `AllowOther` in [`fuse.go`](https://github.com/juicedata/juicefs/blob/main/pkg/fuse/fuse.go)). When mounting by non-root users, you'll need to first modify `/etc/fuse.conf` and enable `user_allow_other`, and then add `allow_other` to the mount command.

## writeback_cache

:::note
This mount option requires at least version 3.15 Linux kernel
:::

FUSE supports ["writeback-cache mode"](https://www.kernel.org/doc/Documentation/filesystems/fuse-io.txt), which means the `write()` syscall can often complete rapidly. It's recommended to enable this mount option when write small data (e.g. 100 bytes) frequently.

## FUSE dirty-page ratio

On Linux, `--fuse-max-dirty-ratio` controls the maximum percentage of the system-wide dirty-page budget available to each JuiceFS FUSE mount. The default is `50`; use `0` to retain the kernel default. This option changes only the upper bound and does not reserve memory or guarantee that the mount receives the configured share.

The client applies the value through the mount's BDI sysfs entry after mounting and reads the value back for logging. This requires sufficient permission; if the setting or readback fails, the mount continues with a warning. Multiple mounts still share the system-wide dirty-page limit.

## Fast Write (experimental)

`--fast-write-dir` keeps unpublished file pages in persistent local sparse files. Enabling it automatically enables the required FUSE `writeback_cache` mode and changes the atime mode to `noatime`. It is intended only for a single-writer workflow where other clients do not access the inode before publish.

Before upload, stop all writers and `fsync` each target file or close every writable FD; the control command cannot drain target-file FUSE dirty pages. Both upload modes checkpoint and seal every selected inode before returning. The default command then materializes the fixed generations in the background; `--wait` also waits until their objects are readable and must be used before publish. All objects for a generation, including JuiceFS writeback staging blocks, are made durable before their slice metadata is published. A failed upload keeps the sealed generation and a retry starts it again without a separate upload journal.

Seal first reflinks the active generation into a temporary directory. If reflink is unavailable, it copies only bitmap-owned 4 KiB pages, then publishes the sealed generation after syncing its files and directories. The original active generation enters retired cleanup only after the sealed copy is durable, so sealing does not freeze the only data copy by moving it directly. Crash recovery handles intermediate sealing and retired states.

The Fast Write directory is not an evictable cache; losing it loses unpublished data. It and the JuiceFS mountpoint must not contain one another, including through symlinks, and the volume must use `trash-days=0`. If trash is enabled after mount, destructive namespace operations fail closed until remount. `O_TMPFILE` follows the normal create-and-unlink path, and writes while it is open use Fast Write. The `juicefs clone` and `juicefs rmr` control commands are rejected because they bypass the normal VFS overlay lifecycle. Upload control requests are accepted only from root or the mount process owner.

The remote inode length is extended immediately when a local write crosses EOF, so other clients can observe the new zero-filled range before upload. A partial backend write is reported as a short write and the remote length is reduced to the committed range. Local capacity is managed by XFS free space or project quota rather than by a Fast Write directory scan. Shrinking truncate, including `O_TRUNC` on a non-empty file, returns `EOPNOTSUPP`. Linux 5.4 does not forward `FALLOC_FL_ZERO_RANGE` to FUSE, so zero-range also returns `EOPNOTSUPP` and applications must write zeroes instead; punch-hole is supported. Only `fsync` checkpoints local pages; close and FUSE flush do not imply local durability. Fast Write uses the v1 local disk format and refuses unknown magic or versions.

`--fast-write-sync-concurrency` limits the aggregate number of chunk `fdatasync` calls that may run concurrently across all inodes in one mount. It defaults to `16`; non-positive values are reset to that default. Each file `fsync` still waits for all of its dirty chunks; the option controls only backend concurrency and does not change durability semantics. Values that are too high can increase device queue contention and tail latency, while values that are too low can reduce throughput.

## user_id and group_id

These options are used to specify the owner ID and owner group ID of the file system (as distinct from the UID and GID of a file or directory) for higher-level permission validation. If the allow_other option is specified, this option will not work. e.g. `sudo juicefs mount -o user_id=100,group_id=100`.

## ReadDirPlusAuto {#readdirplusauto}

Starting from JuiceFS v1.4, `ReadDirPlusAuto` is automatically enabled. This feature allows the FUSE kernel module to automatically decide whether to use the `ReadDirPlus` operation (which returns directory entries along with file attributes) instead of plain `ReadDir`. This reduces the number of subsequent `getattr` calls when listing directories, significantly improving performance for large directory listings.

This is an internal optimization and does not require any user configuration.

## debug

This option will output Debug information from the low-level library (`go-fuse`) to `juicefs.log`.

:::note
This option will output debug information for the low-level library (`go-fuse`) to `juicefs.log`. Note that this option is different from the global `-debug` option for the JuiceFS client, where the former outputs debug information for the `go-fuse` library and the latter outputs debug information for the JuiceFS client. see the documentation [Fault Diagnosis and Analysis](../administration/fault_diagnosis_and_analysis.md).
:::
