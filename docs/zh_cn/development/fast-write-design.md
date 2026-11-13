# Fast Write 两阶段写入

## 适用场景

该实验功能面向单写客户端、publish 后多客户端只读的工作负载，例如轻量虚拟机内存文件、快照、压扁后的 rootfs 和可写盘。

publish 前，同一 inode 不允许被其他客户端访问或修改。本功能不提供分布式写租约，也不提供整文件远端原子可见性。

## 挂载

```bash
juicefs mount META_URL /mnt/jfs \
  --fast-write-dir=/var/lib/juicefs/fast-write \
  --fast-write-sync-concurrency=16
```

- Fast Write 会自动开启必需的 FUSE `writeback_cache`，并把 atime 模式调整为
  `noatime`；调用方不需要再重复传入这两个选项。
- JuiceFS `--writeback` 是另一项配置，决定 materialize 后的普通 slice 是先进入 staging，还是直接上传对象存储。
- Fast Write 目录是 publish 前唯一的数据副本，必须位于持久化本地磁盘，不能放入可淘汰 cache。
- Fast Write 目录与当前 JuiceFS mountpoint 不能存在任一方向的包含关系；直接重叠和 symlink 重叠都会在挂载前被拒绝。
- 一个 Fast Write 目录只能由一个挂载进程持有。
- 卷必须配置 `trash-days=0`；挂载后动态开启 trash 时，reload 与普通文件的
  unlink/rename/link/release 生命周期转换使用同一把锁，并永久使 unlink 和 rename
  fail closed，直到重新挂载。Mknod、Mkdir、Rmdir、Symlink 和 Create 不接触本地
  overlay，不获取该锁。

- `--fast-write-sync-concurrency`：限制整个挂载并发执行的 chunk `fdatasync` 数量，默认
  `16`；非正数会恢复为默认值。

## 本地格式

v1 文件按 64 MiB chunk 划分。每个 chunk 文件由两部分组成：

- 前 64 MiB 是本地稀疏数据；
- 文件末尾 4 KiB 是带版本、generation、chunk index 和 CRC 的 footer，其中包含一个 16,384 bit（2 KiB）的 4 KiB 页 bitmap。

本地稳定状态使用 `active-ID` 和 `sealed-ID` 表达 generation。seal 事务还会短暂使用
`.sealing-ID` 和 `.retired-ID`；上传成功后的清理使用 `.gc-ID`。根目录锁文件记录格式
版本和 volume UUID；未知格式会拒绝挂载。

bitmap 是正确性依据：

- bit=1：该页由本地 generation 接管；
- bit=0：读取更老的本地 generation，最后回退远端 slice；
- bit=1 且稀疏文件对应位置为 hole：表示显式全零页。

因此 upload 不依赖 `SEEK_DATA` 判断有效数据。非 4 KiB 对齐写在首次接管页面时先从下层读取整页，再执行 RMW。

底层只完成部分页面时，Fast Write 只发布完整新页（已有页按实际用户字节计算），FUSE 返回 short write，远端长度回退到实际提交范围。本地容量直接由 XFS 空闲空间或 project quota 管理，不在 Fast Write 热路径扫描目录。

## flush、fsync、generation 与恢复

普通 write 只更新数据和内存 bitmap。FUSE flush/close 会在 inode mutation guard 内对齐
远端 length/mtime，但不保证本地页面持久化，也不创建 generation。metadata dirty 使用
mutation epoch，旧 flush 不会清除并发新写。用户调用 fsync 时：

1. 持有同 inode mutation guard，先提交远端 length/mtime；已 unlink 但仍打开的 inode
   不再访问远端 metadata，只 checkpoint 本地数据；
2. 在挂载级并发上限内对 dirty chunk 执行 `fdatasync`，持久化页面数据；
3. 将最新 bitmap 写入每个 dirty chunk 的 footer，再执行一次 `fdatasync`；
4. 按 generation、inode、inodes 的顺序同步目录。

多个 inode 同时 `fsync` 时共享同一个并发预算；每个调用必须等待属于该 inode 的全部 dirty chunk 及目录同步成功后才能返回。同步失败不会清除 dirty 状态。数据先于 bitmap 的两阶段持久化顺序确保掉电恢复不会接受指向未落盘页面的有效 footer。

`fsync` 返回时 active 目录保持不变，不执行 upload。为了避免后续覆盖原地修改已经被旧
footer 发布的页面，checkpoint 后第一次新 mutation 会先 seal 旧 active，再写入新
generation；这一步只隔离本地恢复边界，不会上传对象或提交 slice。显式 upload 仍负责
checkpoint 当前 active 并固定完整上传边界。

seal 不再把 active 数据目录直接改名为 sealed，而是执行以下事务：

1. 创建 `.sealing-ID`，对每个固定大小的 chunk 文件优先执行整文件 reflink；
2. 文件系统不支持 reflink 时，按 bitmap 只复制 bit=1 的 4 KiB 页。全零页不写数据块，
   但 footer 中的 bit 仍为 1，因此逻辑零页保持有效且目标文件继续稀疏；
3. 对目标 chunk 执行 `fdatasync` 并同步 `.sealing-ID` 后，原子发布为 `sealed-ID`；
4. 将原 `active-ID` 改名为 `.retired-ID`，创建下一代 `active-ID`，最后删除 retired
   副本并同步 inode 目录。

整个过程持有 inode 写锁，读写不会观察到中间目录状态。创建新 active 前的复制或发布错误
会回滚到旧 active；最终目录同步失败会保留可重试、可恢复的已提交边界。启动恢复会删除
未发布的 `.sealing-ID`、处理 `.retired-ID`，并对同时存在的 active/sealed 同 ID 逐
chunk 校验 footer 和 bitmap 有效页后回滚重复 clone。不匹配的重复 generation 会 fail
closed。

进程重启扫描带 CRC 的 footer；未 fsync 的新增页可能丢失。恢复句柄状态成功后，再按远端
`Nlink` 调和 `.deleted` marker：仍有恢复句柄的 unlinked inode 保留到最后 close，已经
重新获得链接的 inode 删除 marker，真正孤儿才清理。损坏 footer、句柄状态、未知格式和
volume UUID 不匹配都会使挂载 fail closed，不会静默返回零或消费恢复文件。

## upload 与 publish

```bash
# 同步 checkpoint + seal 全部目标 inode，随后异步 materialize
juicefs upload-fast-write /mnt/jfs/sandbox

# publish 前等待本次 slice 对象真正可读
juicefs upload-fast-write --wait /mnt/jfs/sandbox
```

upload 按 generation 顺序扫描 bitmap，将连续置位页面转换为现有普通 dense slice。对象和元数据格式没有变化。

- 两种命令都会在返回前同步 checkpoint + seal 全部目标 inode，固定本次 generation 边界；默认命令只把后续 materialize/upload 放到后台。
- 未启用 JuiceFS `--writeback`：先完成该 generation 的全部对象 Put，再执行 `meta.Write`。
- 启用 JuiceFS `--writeback`：先把该 generation 的全部 dense slice 持久化到 staging 并强制上传，再执行 `meta.Write`。
- `--wait` 会强制上传并等待本次 staging 对象完成，应用只能在该命令成功后 publish。

upload 不维护额外 journal。失败时保留 sealed generation；重试重新扫描 bitmap、分配 SliceID 并从头上传。对象阶段失败只会留下可由 GC 清理的孤儿对象；metadata 阶段失败后重试可能留下被新记录覆盖的重复 slice，由社区 compact/GC 处理。只有 `--wait` 成功才允许 publish。materialize 使用零 mtime 写入语义，不会覆盖并发新 generation 或显式设置的 inode mtime。

全部 metadata 已提交后，generation 先原子改名为 `.gc-ID` 并从合并读视图摘除，再执行
best-effort 删除。因此清理中途失败不会让读路径重新依赖一个只删除了一部分的目录；残留
目录由下次启动继续清理。

## copy、truncate 与空洞

- 普通 read/write copy 自动经过合并视图。
- `copy_file_range` 在同 inode mutation guard 内完成长度快照、预扩展、逐页复制和失败
  回滚；4 KiB 对齐的本地页优先使用底层文件系统 reflink，不支持时回退逻辑拷贝。
- 远端 inode 是文件长度的权威来源。write、fallocate 和 copy 越过 EOF 时先立即扩展远端长度，因此其他客户端在 upload 前会看到扩展范围为零。
- 已 unlink 但仍有打开句柄的 inode 是例外：远端已无可扩展的 namespace inode，后续越
  EOF 写只增长本地 overlay；`fstat` 和 graceful-restart 句柄状态都使用该本地长度。
- short write/copy 只保留实际提交范围；提交 0 字节时恢复操作前长度。零长度
  `copy_file_range` 不修改长度、mtime 或配额。
- Fast Write 启用时不支持 truncate 缩短，包括对非空文件使用 `O_TRUNC`，返回 `EOPNOTSUPP`；只允许长度不变或增长。
- punch-hole 使用显式零页语义；支持把 `FALLOC_FL_ZERO_RANGE` 转发给 FUSE 的内核也使用同样语义。Linux 5.4 会在内核中直接拒绝 FUSE zero-range（请求不会到达 JuiceFS），此时返回 `EOPNOTSUPP`，应用需要改用写零。
- rename 和 hardlink 继续按 inode 工作；最后一个链接删除后，已打开 inode 的 Fast Write 状态保留到最后一个 handle 关闭。
- `O_TMPFILE` 继续走社区创建路径：生成临时名字、打开句柄后立刻 unlink。Fast Write 不拦截这次创建。inode 在关闭前仍保留，写入照常进入本地 overlay，并立即扩展远端长度。`link` 把它接回目录后，关闭只解除 sustained 状态，inode 和 overlay 都保留；最后一次关闭时如果没有链接，则删除 inode 和这段本地数据。
- `juicefs clone` 和 `juicefs rmr` 会绕过普通 VFS overlay 生命周期，因此 Fast Write 挂载明确拒绝这两个控制命令；文件内复制和普通 `cp`/`rm -rf` 仍走合并视图。

## 运维约束

- 本地盘损坏会丢失尚未 upload 的数据。
- publish 前必须先停止写入，并逐文件 `fsync` 或关闭所有 writable FD；控制命令不能代替内核排空目标文件的 FUSE 脏页。
- 异步命令返回表示 generation 已 seal、后台任务已调度，不表示对象存储已经可读；publish 仍必须使用 `--wait`。
- upload 控制请求只允许 root 或挂载进程所属 UID 触发。
- 回滚到不支持该功能的客户端前，必须执行 `upload-fast-write --wait` 并确认成功。
- 使用 XFS 容量/project quota 监控本地空间，并监控 `juicefs_fast_write_files`。
- 当前只支持 Linux 挂载。

Fast Write 控制命令使用 fork 私有高位编号 `0xF0000001`，避免与社区版顺序分配的命令编号冲突；
控制命令与挂载进程必须使用同一版本。
磁盘格式为 v1，使用 Fast Write 专用 magic；未知 magic 或版本会拒绝挂载，避免静默误读本地数据。

## 验证

`tests/fast-write/` 提供完整的 Linux/FUSE 验证入口：

- Go 单元、随机模型、checkpoint 后覆盖恢复、双 FD mutation 并发、graceful restart、
  short write/copy、清理故障注入和 race 测试；
- 当前工作区与社区基线二进制的可重复构建；
- 真实 FUSE 的 `fsync` 后崩溃恢复、同步/异步 upload、writeback staging 和社区客户端读取；
- XFS、社区版和 Fast Write 的固定 fio 性能矩阵。

性能摘要同时报告 fio 统计区间内的 IOPS，以及包含进程退出和 FUSE close/flush 等待的
effective IOPS。buffered 无 fsync 只衡量前台吸收脏页能力；发布性能必须单独查看
逐文件 fsync 和 `upload-fast-write --wait`。
