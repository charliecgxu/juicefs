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

- bit=1：该页由当前稀疏文件接管；
- bit=0：当前文件没有这一页。若存在一份已经 rename 出去、正在上传的文件，再查那一份，否则回退远端 slice；
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

`fsync` 和 `flush` 都在原来的 active 文件上 checkpoint，不 rotate，也不上传。之后的写入继续落在同一个文件上。命令行上传直接提交这个文件，成功后删除它并留下一个空的 active，供后续写入使用。已经存在一份 sealed 文件时，不再 clone 出第三份。

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
# 直接上传选中路径的当前稀疏文件，返回时后台任务已调度
juicefs upload-fast-write /mnt/jfs/sandbox

# 等待选中路径的 slice 提交完成
juicefs upload-fast-write --wait /mnt/jfs/sandbox

# 提交这个挂载上的全部稀疏文件，完成后才返回。umount 不会做这件事。
juicefs upload-fast-write --all /mnt/jfs
```

upload 按 generation 顺序扫描 bitmap，将连续置位页面转换为现有普通 dense slice。对象和元数据格式没有变化。

- 命令直接上传当前稀疏文件，不先 clone 出一份。`--wait` 和 `--all` 在返回前等待 slice 提交完成；不带这两个参数时，materialize 放到后台。umount 只 checkpoint，不上传。
- 未启用 JuiceFS `--writeback`：先完成该 generation 的全部对象 Put，再执行 `meta.Write`。
- 启用 JuiceFS `--writeback`：`writer.Finish` 把 dense slice 写入 staging，然后按社区写路径提交 `meta.Write`。对象由后台按 `--upload-delay` 和上传窗口上传。
- `--wait` 等待本次 `meta.Write` 完成。其他客户端要等到 staging 对象上传完成后才能读到，这与社区 `--writeback` 相同，由应用自行处理。

upload 不维护额外 journal。失败时保留当前稀疏文件；重试重新扫描 bitmap、分配 SliceID 并从头上传。对象阶段失败只会留下可由 GC 清理的孤儿对象；metadata 阶段失败后重试可能留下被新记录覆盖的重复 slice，由社区 compact/GC 处理。`--wait` 和 `--all` 成功后才删除已提交的本地文件。materialize 使用零 mtime 写入语义，不会覆盖显式设置的 inode mtime。

全部 metadata 已提交后，generation 先原子改名为 `.gc-ID` 并从合并读视图摘除，再执行
best-effort 删除。因此清理中途失败不会让读路径重新依赖一个只删除了一部分的目录；残留
目录由下次启动继续清理。

## copy、truncate 与空洞

- 普通 read/write copy 自动经过合并视图。
- `copy_file_range` 先复制 inode 长度以内的 slice 引用。对齐的本地页再 reflink 到目标稀疏文件；源没有本地页的位置清掉目标 bitmap。不对齐的头尾按页读写。
- 文件长度的更新时机与社区一致。`write` 只推进本进程可见的本地长度；`flush`、`fsync` 和 `close` 把这个长度写入 inode，但不提交 slice 或对象。`truncate` 和 `fallocate` 扩容仍在系统调用内更新 inode。upload 前没有其他客户端读写该文件，因此 inode 长度可以先于 slice 生效。
- 已 unlink 但仍有打开句柄的 inode 在 `flush`、`fsync` 和 `close` 时同样更新仍存在的 inode 长度；inode 已经不存在时只保留本地长度。
- short write 只把本地长度停在实际写完的位置，inode 长度留到下一次 `flush`、`fsync` 或 `close`。short copy 把 inode 长度收敛到实际复制范围。零长度 `copy_file_range` 不修改长度、mtime 或配额。
- Fast Write 启用时不支持 truncate 缩短，包括对非空文件使用 `O_TRUNC`，返回 `EOPNOTSUPP`；只允许长度不变或增长。
- punch-hole 使用显式零页语义；支持把 `FALLOC_FL_ZERO_RANGE` 转发给 FUSE 的内核也使用同样语义。Linux 5.4 会在内核中直接拒绝 FUSE zero-range（请求不会到达 JuiceFS），此时返回 `EOPNOTSUPP`，应用需要改用写零。
- rename 和 hardlink 继续按 inode 工作；最后一个链接删除后，已打开 inode 的 Fast Write 状态保留到最后一个 handle 关闭。
- `O_TMPFILE` 继续走社区创建路径：生成临时名字、打开句柄后立刻 unlink。Fast Write 不拦截这次创建。inode 在关闭前仍保留，写入进入本地 overlay，长度在 `flush`、`fsync` 或 `close` 时写入该 inode。`link` 把它接回目录后，关闭只解除 sustained 状态，inode 和 overlay 都保留；最后一次关闭时如果没有链接，则删除 inode 和这段本地数据。
- `juicefs clone` 和 `juicefs rmr` 会绕过普通 VFS overlay 生命周期，因此 Fast Write 挂载明确拒绝这两个控制命令；文件内复制和普通 `cp`/`rm -rf` 仍走合并视图。

## 运维约束

- 本地盘损坏会丢失尚未 upload 的数据。
- publish 前必须先停止写入，并逐文件 `fsync` 或关闭所有 writable FD；控制命令不能代替内核排空目标文件的 FUSE 脏页。
- 不带 `--wait` 或 `--all` 的命令返回时，后台任务已调度，不表示对象存储已经可读。发布前使用 `--all` 或 `--wait`。
- upload 控制请求只允许 root 或挂载进程所属 UID 触发。
- 回滚到不支持该功能的客户端前，必须执行 `upload-fast-write --all` 并确认成功。umount 只把稀疏文件落盘，不等待上传。
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
