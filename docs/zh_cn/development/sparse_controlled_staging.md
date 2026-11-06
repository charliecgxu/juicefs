# 稀疏可控暂存设计与实现

本文描述 `opt2` 分支中 `--sparse-chunk-slice` 的设计目标、磁盘与对象格式、
读写流程、故障语义和当前限制。本文以当前代码为准，不代表社区版 JuiceFS 的行为。
相对社区版的逐项代码变更参见
[稀疏暂存优化相对社区版的代码变更说明](sparse_staging_community_diff.md)。

## 背景

社区版按不可变 slice 写入数据。随机小写会不断创建 slice，并更新中心元数据；
slice 达到一定数量后还会触发 compact。这个模型具有良好的多客户端可见性，但在
单写者、频繁随机覆盖的场景中会产生以下开销：

- 每次小写都可能创建 slice、对象和中心元数据记录；
- compact 需要重新读取、合并和上传数据；
- 只修改少量 4 KiB 页面也可能造成较大的写放大；
- 写入路径依赖对象存储延迟和中心元数据延迟。

本实现针对以下受约束的工作负载：

- 同一文件在一次发布周期内只有一个写者；
- 应用负责决定何时发布（Put）；
- 发布完成前，其他客户端不会读取该文件的新数据；
- `fsync` 要求本地暂存持久化并登记中心元数据，但不要求数据可被其他客户端读取；
- 可以在暂存空间达到高水位时执行保护性发布。

这些约束目前没有由 JuiceFS 强制实施。违反约束可能导致其他客户端读到旧数据，
或者多个写者之间发生最后写入者覆盖。

## 设计目标

1. 一个发布周期内，每个 `(inode, 64 MiB chunk)` 通常只有一个本地 generation。
2. 对同一 generation 的随机写直接覆盖本地暂存文件，不创建新的中心 slice。
3. 未写入的页面始终保持为洞，不在本地和对象存储中填零占用空间。
4. `fsync` 后，同一节点重启可以从本地暂存目录恢复数据和位图。
5. 首次 `fsync` 登记一个稳定的中心 slice，后续 `fsync` 幂等复用；发布只上传对象。
6. 已发布对象保持不可变；发布期间的新写进入新的 active generation。
7. 未启用该功能时保持社区版写入行为。

## 非目标

当前实现不提供：

- 发布前的跨客户端可见性；
- 本地暂存盘损坏或丢失后的数据恢复；
- 多写者冲突检测或分布式写锁；
- sparse generation 的 compact；
- sparse 对象的完整 GC 计费和扫描；
- `truncate`、`fallocate` 和 `copy_file_range` 支持。
- Put 前对文件执行 metadata clone；clone 共享 slice ID，但新 inode 不共享原 inode
  的本地 staging overlay，必须先完成 Put。

## 启用方式

该功能会引入社区客户端无法识别的对象格式，必须先执行一次不可逆的格式升级：

```shell
juicefs config META-URL --enable-sparse-chunk
```

随后挂载时启用 writeback 和稀疏 chunk slice：

```shell
juicefs mount META-URL MOUNTPOINT \
  --writeback \
  --sparse-chunk-slice \
  --cache-dir /path/to/cache
```

`--sparse-chunk-slice` 要求磁盘 cache，不能使用纯内存 cache。元数据格式从版本 1
升级到版本 2，使不支持 packed sparse slice 的旧客户端拒绝挂载，而不是错误解析。

显式发布使用：

```shell
# 异步发布指定文件
juicefs flush-staging MOUNTPOINT/path/to/file

# 等待上传和元数据发布完成
juicefs flush-staging --wait MOUNTPOINT/path/to/file

# 发布该挂载点的全部暂存数据
juicefs flush-staging --wait MOUNTPOINT
```

## 核心模型

### Generation

每个 generation 对应一个 inode 的一个 64 MiB chunk，包含：

- inode 和 chunk index；
- 带稀疏标记的唯一 slice ID；
- 文件写入前后的 EOF；
- 4 KiB 粒度的 data bitmap；
- 4 KiB 粒度的 zero bitmap；
- 本地 `.stage` 文件；
- active 或 frozen 状态。

页面状态有三种：

- `inherit`：两个 bitmap 都未置位，读取时继承更老的 generation 或旧 slice；
- `data`：data bitmap 置位，页面内容保存在暂存文件或 packed payload 中；
- `zero`：zero bitmap 置位，页面逻辑内容为零，但不保存 4 KiB 零数据。

一个 chunk 共 16384 个 4 KiB 页面，因此每个位图占 2048 字节。

### Active 与 pending

正常写入复用 active generation。发布开始时，active generation 被 checkpoint 并
冻结，然后移入 pending。这样发布不可变快照的同时，后续写入可以创建新的 active
generation。pending 按 slice ID 顺序发布，以保持覆盖顺序。

只要应用在一次发布完成前不继续写，通常一个 chunk 只有一个 generation 和一个
最终的全局 sparse slice。跨越多次发布后会产生多个不可变 sparse slice；当前尚未
实现它们之间的 compact。

## 本地 `.stage` 文件格式

暂存文件位于第一个 cache 目录下的 `sparse-staging/`：

```text
<inode>-<chunk-index>-<sparse-slice-id>.stage
```

文件布局如下：

```text
0                  8 KiB                16 KiB
+-------------------+-------------------+--------------------------+
| header slot 0     | header slot 1     | 64 MiB sparse data area  |
+-------------------+-------------------+--------------------------+
```

文件逻辑长度是 `16 KiB + 64 MiB`，但数据区通过按偏移写入保持稀疏。没有写过的
页面是文件系统洞，不占用对应的物理空间。

每个 8 KiB header slot 保存：

- magic 和格式版本；
- frozen 标记；
- 单调递增 sequence；
- inode、chunk index 和 sparse slice ID；
- logical size、base EOF 和 real EOF；
- mtime；
- data bitmap 和 zero bitmap；
- CRC32。

两个 header slot 交替写入。恢复时校验 magic、版本和 CRC，并选择 sequence 最大的
有效 header。这样即使写 header 时掉电，通常仍可使用上一个完整 checkpoint。

本实现没有逐写 journal。普通 `write` 只更新数据区和内存位图；`fsync`、`flush`
或 `close` 才执行 checkpoint：

1. `fsync` 暂存文件，使数据页先持久化；
2. 将新 header 写入另一个 slot；
3. 再次 `fsync` 暂存文件；
4. `fsync` 暂存目录。

因此，最后一次成功 checkpoint 之后、下一次 checkpoint 之前的写入在故障后可能
丢失，这与 `fsync` 的边界一致。

## 写入流程

写入由 `pkg/vfs/sparse_staging.go` 处理：

1. 根据文件偏移定位 64 MiB chunk。
2. 查找该 `(inode, chunk)` 的 active generation；不存在时分配 sparse slice ID
   并创建 `.stage` 文件。
3. 对完整且对齐的 4 KiB 页面直接执行覆盖写，不读取旧页面。
4. 对非完整页面执行 read-modify-write：
   - 当前 generation 已包含该页面时，从 `.stage` 读取；
   - 页面为 zero 时以零为基础；
   - 否则从中心元数据表示的旧数据和 pending generation 合成基础页。
5. 全零页面只设置 zero bitmap；非零页面写入数据区并设置 data bitmap。
6. 更新内存中的 EOF 和 mtime。

部分页写入必须先读取旧页面，因为 POSIX 写语义要求保留此次写入范围之外的旧字节。
完整 4 KiB 覆盖不存在这个开销。

普通写入不会立即更新中心元数据。`fsync`、`flush` 或 `close` checkpoint 成功后，
使用稳定的 sparse slice ID 调用 `WriteEOF`：首次调用登记一条覆盖完整 chunk 的
slice，后续调用只更新 EOF 和时间戳，不追加重复 slice。Redis、SQL 和 TKV 后端都
校验重复 ID 的记录内容必须一致，否则返回 `EINVAL`。

## 本地读取

同一挂载点读取时：

1. 先按中心元数据读取已发布数据；仍存在于本地 staging 的 sparse ID 暂不从对象
   存储读取；
2. 再按 generation 顺序覆盖 pending 和 active 的本地页面；
3. inherit 页面保留基础数据，zero 页面清零，data 页面从 `.stage` 读取。

因此，写后本地读可以看到未发布内容。中心元数据在 `fsync` 后已经包含新 ID 和
EOF，但对应 manifest 在 Put 前并不存在；其他客户端此时读取会失败，而不是静默
返回旧数据。应用必须遵守“Put 完成前其他客户端不读取”的约束。

## 重启恢复

挂载初始化时扫描 `sparse-staging/*.stage`：

1. 检查文件逻辑长度；
2. 读取并校验两个 header slot；
3. 选择 sequence 最大的有效 checkpoint；
4. 根据 frozen 标记恢复到 active 或 pending；
5. 同一 key 出现多个 active 时，ID 最大的保留为 active，其余转为 pending。

恢复依赖同一个 cache 目录及其完整内容。尚未 checkpoint 的首次 generation 没有
有效 header，恢复时会被删除。`fsync` 成功后，只要本地 staging 盘仍可用，数据、
位图和本地 EOF 可以恢复。恢复时还会用相同 ID 幂等登记中心元数据；如果 inode
已经被删除，则把该 stage 视为过期文件并清理。

## 发布流程

`flush-staging` 调用发布逻辑：

1. 将匹配 inode 的 active generation 标记为 frozen；
2. checkpoint、幂等登记中心元数据并移入 pending；
3. 将所有 data 页面按页号顺序打包上传；
4. 上传 sparse manifest；
5. 使本地读缓存失效；
6. 从 pending 移除 generation，并删除对应 `.stage` 文件。

Put 不再追加或修改 slice 记录，只负责把该 ID 对应的 payload 和 manifest 上传。
上传失败时本地 generation 保留并可重试。由于 slice 在 `fsync` 已经登记，Put 前
远端客户端不能读取它；这是本模式依赖应用访问时序换取简单、低延迟写路径的明确
一致性边界。

除了显式命令，暂存数据量达到
`cache-size * stage-usage-high-ratio` 时也会异步调用全量发布。将
`--stage-usage-high-ratio` 设为 `0` 可以关闭该保护机制。

## 对象存储格式

Sparse slice ID 使用正整数的 bit 62 作为格式标记。去掉该位后仍是正常分配的
slice ID，因此不会占用 signed 64-bit SQL 字段的符号位。

### Payload

只有 data bitmap 中置位的页面会上传。页面按逻辑页号升序紧密排列，每个 payload
part 最大 4 MiB。inherit 页面和 zero 页面都不占 payload 空间。

对象 key 以 `<slice-id>_sparse` 为前缀，payload part 的 key 还包含 part 编号和
实际大小。

### Manifest

manifest 保存：

- magic 和版本；
- 4 KiB page size；
- 逻辑 chunk size；
- 4 MiB payload part size；
- data bitmap 和 zero bitmap；
- 每个 payload part 的实际大小；
- CRC32。

读取 manifest 时会构建 data bitmap 的字节级 prefix rank。读取某个逻辑页面时，
通过 rank 在 O(1) 时间内计算其 packed payload 偏移，避免每次从 bitmap 起点扫描。

## 全局读取与 overlay

普通 JuiceFS `buildSlice` 会消除被新 slice 覆盖的旧范围。Sparse slice 的 inherit
页面却需要继续读取更老的数据，因此元数据层新增了 overlay 读取形式，保留持久化
写入顺序和每条 slice 的 chunk 位置。

当 chunk 含 sparse slice 时：

1. 元数据层返回有序 overlay records；
2. 读取器按顺序应用 dense slice、sparse data 和 sparse zero；
3. sparse inherit 页面不修改已有结果；
4. 单 sparse slice 且请求按 4 KiB 对齐时走批量快速路径，直接读入目标 buffer。

manifest 和 prefix rank 会在内存中缓存。当前 sparse chunk 会跳过社区版 dense
compact，因为 dense compact 不理解 inherit 语义。

## 元数据与兼容性

中心元数据中的 slice 记录格式没有增加字段。Sparse slice 通过 ID bit 62 区分，
其 `Len` 和 `Size` 都使用完整 64 MiB；`Size` 是删除回调和格式识别使用的稳定逻辑
值，不代表 manifest 加 payload 的物理大小。
`WriteEOF` 单独传递真实文件 EOF，避免 64 MiB 逻辑覆盖错误扩展文件长度。

dump、backup 和 replay 在计算 next slice ID 时会去掉 sparse 标记位。

元数据格式版本 2 是兼容性边界：

- 新客户端可以识别 sparse ID、manifest 和 overlay；
- 未修改的社区客户端不应挂载格式版本 2；
- 格式升级不能通过命令回退。

## 故障与一致性语义

- `write` 成功但没有 `fsync`：进程或节点故障后允许丢失最近写入。
- `fsync` 成功：同一 cache 目录可恢复本地数据，中心元数据已有稳定 slice 和 EOF；
  不保证远端对象存在。
- 节点及其 staging 盘同时丢失：未发布数据无法恢复。
- 发布前其他客户端读取：看到元数据但因 manifest 尚未上传而失败。
- 对象上传中故障：本地 stage 和中心 slice 保留，重试使用相同对象 key。
- Put 成功：其他支持格式版本 2 的客户端可以读取新 generation。
- inode 最后一条链接删除后，常规 `DeleteSlice` 链路会同时清理未上传 `.stage`
  和该 ID 前缀下可能存在的 manifest、payload 或部分上传对象；open-unlink 仍遵循
  社区版 sustained inode 语义，等最后一个打开句柄关闭后再清理。

## 当前限制与后续工作

1. 实现 sparse-aware compact，合并多次发布产生的 generation，同时保留洞。
2. 将 sparse manifest 和 payload 纳入 `juicefs gc` 引用扫描及空间统计。
3. 接入常规 block compression、上传限速和 tier 选择。
4. 实现安全的 truncate、fallocate 和 copy-file-range 语义。
5. 增加多写者保护，或明确实现租约/冲突检测。
6. 增加暂存盘故障、发布中断和多 generation 的故障注入测试。
7. 如需解除 Put 前禁止跨客户端读取的约束，应增加独立的中心“本地暂存状态”或
   两阶段可见性协议。

在这些工作完成前，该功能应保持实验性并仅用于满足本文约束的工作负载。
