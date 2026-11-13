---
title: FUSE 挂载选项
sidebar_position: 5
slug: /fuse_mount_options
---

JuiceFS 文件系统为用户提供多种访问方式，FUSE 是其中较为常用的一种，即使用 `juicefs mount` 命令将文件系统挂载到本地的方式。用户可以根据需要添加 FUSE 支持的挂载选项，从而实现更细粒度的控制。

本指南介绍 JuiceFS 常用的 FUSE 挂载选项，有两种添加挂载选项的方式：

1. 手动执行 [`juicefs mount`](../reference/command_reference.mdx#mount) 命令时，通过 `-o` 选项指定，多个选项使用半角逗号分隔。

   ```bash
   juicefs mount -d -o allow_other,writeback_cache sqlite3://myjfs.db ~/jfs
   ```

2. Linux 发行版通过 `/etc/fstab` 定义自动挂载时，在 `options` 字段处直接添加选项，多个选项使用半角逗号分隔。

   ```
   # <file system>       <mount point>   <type>      <options>           <dump>  <pass>
   redis://localhost:6379/1    /jfs      juicefs     _netdev,writeback_cache   0       0
   ```

## default_permissions

JuiceFS 在挂载时会自动启用该选项，无需显式指定。该选项将启用内核的文件访问权限检查，它会在文件系统之外进行，启用后，内核检查和文件系统检查必须全部成功才允许进一步操作，该选项通常与 `allow_other` 一起使用。

:::tip
内核执行的是标准的 Unix 权限检查，基于 mode bits、UID/GID、目录所有权。
:::

## allow_other

FUSE 默认只有挂载文件系统的用户可以访问挂载点中的文件，`allow_other` 选项可以让其他用户也可以访问挂载点上的文件。当 root 用户挂载时，该选项会自动启用（在 [`fuse.go`](https://github.com/juicedata/juicefs/blob/main/pkg/fuse/fuse.go) 搜索 `AllowOther` 字样），无需显式指定。而如果是普通用户挂载，则需要修改 `/etc/fuse.conf`，在该配置文件中开启 `user_allow_other` 配置选项，才能在普通用户挂载时启用 `allow_other`。

## writeback_cache

:::note 注意
该挂载选项仅在 Linux 3.15 及以上版本内核上支持
:::

FUSE 支持[「writeback-cache 模式」](https://www.kernel.org/doc/Documentation/filesystems/fuse-io.txt)，这意味着 `write()` 系统调用通常可以非常快速地完成。当频繁写入非常小的数据（如 100 字节左右）时，建议启用此挂载选项。

## FUSE 脏页比例

在 Linux 上，`--fuse-max-dirty-ratio` 控制每个 JuiceFS FUSE 挂载最多可以使用全局脏页预算的百分比。默认值为 `50`；设置为 `0` 时保留内核默认值。该选项只提高上限，不会预留内存，也不保证挂载一定获得配置的份额。

客户端挂载完成后通过该挂载对应的 BDI sysfs 文件应用配置，并回读实际值写入日志，因此需要足够的权限。如果设置或回读失败，挂载会继续运行并输出警告。多个挂载仍然共享系统的全局脏页上限。

## Fast Write（实验功能）

`--fast-write-dir` 将尚未 publish 的文件页保存在持久化本地稀疏文件中。启用后会自动开启必需的 FUSE `writeback_cache` 并把 atime 模式调整为 `noatime`。该功能仅适用于单写客户端且 publish 前其他客户端不会访问相同 inode 的工作流。

upload 前必须停止写入，并逐文件 `fsync` 或关闭全部 writable FD；控制命令不能替目标文件排空 FUSE 脏页。默认命令和 `--wait` 都会在返回前 checkpoint 并 seal 全部目标 inode：默认命令随后在后台 materialize 固定 generation，publish 前则必须使用 `juicefs upload-fast-write --wait PATH` 等待对象真正可读。一个 generation 的全部对象（包括 JuiceFS writeback staging block）持久化后才提交 slice metadata；失败时保留 sealed generation，下次不依赖额外 journal，直接从头重试。

seal 会先把 active generation reflink 到临时目录；不支持 reflink 时按 bitmap 只复制有效 4 KiB 页，再经过文件和目录同步原子发布 sealed generation。原 active 只在 sealed 已持久化后进入 retired 清理阶段，因此 seal 不会通过直接移动唯一数据副本来固定边界；崩溃恢复会处理 sealing/retired 中间状态。

Fast Write 目录不是可淘汰缓存；丢失该目录会丢失尚未 upload 的数据。该目录与 JuiceFS mountpoint 不能存在任一方向的包含关系（包含 symlink 解析后的路径），卷必须设置 `trash-days=0`；挂载后动态开启 trash 会使破坏性 namespace 操作 fail closed，直到重新挂载。`O_TMPFILE` 与普通创建后立即 unlink 相同，打开期间的写入进入 Fast Write。`juicefs clone` 和 `juicefs rmr` 因绕过普通 VFS overlay 生命周期会被拒绝。upload 控制请求只允许 root 或挂载进程所属 UID 触发。

本地 write 越过 EOF 时会立即扩展远端 inode 长度，因此其他客户端可能在 upload 前看到新增的全零范围。底层部分写会作为 short write 返回，并把远端长度收敛到实际提交范围；本地容量直接由 XFS 空闲空间或 project quota 管理，不在 Fast Write 热路径扫描目录。启用 Fast Write 后，缩短 truncate（包括对非空文件使用 `O_TRUNC`）返回 `EOPNOTSUPP`。Linux 5.4 不会把 `FALLOC_FL_ZERO_RANGE` 请求转发给 FUSE，因此 zero-range 同样返回 `EOPNOTSUPP`，应用应改用写零；punch-hole 正常支持。只有 `fsync` 会 checkpoint 本地页面，close 和 FUSE flush 不代表本地持久化。Fast Write 使用 v1 本地磁盘格式，未知 magic 或版本会拒绝挂载。

`--fast-write-sync-concurrency` 限制一个挂载内所有 inode 合计可以并发执行的 chunk `fdatasync` 数量，默认值为 `16`；非正数会恢复为默认值。每个文件的 `fsync` 仍会等待其所有 dirty chunk 完成同步；该参数只控制底层并发度，不改变持久化语义。过高的值可能增加设备队列争用和尾延迟，过低则可能降低吞吐。

## user_id 和 group_id

这两个选项用来指定文件系统的所有者 ID 和所有者组 ID（不同于文件或目录的 UID、GID），用以做更高层级的权限校验。如果指定了 allow_other 选项，此选项将失效。用法如 `sudo juicefs mount -o user_id=100,group_id=100`。

## ReadDirPlusAuto {#readdirplusauto}

从 JuiceFS v1.4 开始，`ReadDirPlusAuto` 已自动启用。该特性允许 FUSE 内核模块自动决定是否使用 `ReadDirPlus` 操作（在返回目录条目的同时返回文件属性），而非仅使用普通的 `ReadDir`。这减少了目录列表时后续的 `getattr` 调用次数，显著提升了大目录的列举性能。

这是一项内部优化，用户无需进行任何配置。

## debug

该选项会将低层类库（`go-fuse`）的 Debug 信息输出到 `juicefs.log` 中。

:::note 注意
该选项会将低层类库（`go-fuse`）的 Debug 信息输出到 `juicefs.log` 中，需要注意的是，该选项与 JuiceFS 客户端的全局 `--debug` 选项不同，前者是输出 `go-fuse` 类库的调试信息，后者是输出 JuiceFS 客户端的调试信息。详情参考文档[故障诊断和分析](../administration/fault_diagnosis_and_analysis.md)。
:::
