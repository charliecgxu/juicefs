# 稀疏暂存优化相对社区版的代码变更说明

本文逐项说明 `opt2` 分支相对 JuiceFS 社区版 `main` 的代码变化、设计原因、
运行时行为、兼容边界和已知限制。

本文比较的版本为：

- 社区基线：`main`，提交 `4f20009`
- 优化版本：`opt2`，提交 `d08daf5`

相关的完整设计和磁盘格式说明参见
[稀疏可控暂存设计与实现](sparse_controlled_staging.md)。

## 1. 变更规模

相对社区版，当前 `opt2` 的总体差异为：

- 包含文档：新增 3086 行，删除 65 行，净新增 3021 行；
- 排除文档：新增 2757 行，删除 65 行，净新增 2692 行；
- 生产代码：新增 2172 行，删除 65 行，净新增 2107 行；
- 测试代码：新增 585 行。

生产代码按模块拆分：

- `pkg/vfs`：净新增 1181 行；
- `pkg/chunk`：净新增 459 行；
- `pkg/meta`：净新增 272 行；
- `cmd`：净新增 195 行。

生产代码中不包含测试桩、mock 或断言逻辑。测试均位于 `*_test.go`。

## 2. 要解决的社区版问题

社区版把每次写入结果表示为不可变 slice。随机 4 KiB 覆盖写会不断产生新的
slice，并更新中心元数据。当 slice 较多时，后台 compact 会把多个 slice 合并为
新的连续对象。

社区版 compact 对内部洞的处理是写入真实零数据。它只裁掉合并范围首尾的洞，
第一个有效页和最后一个有效页之间的洞会通过 `WriteAt(make([]byte, len))`
物化。因此，在 512 MiB 地址范围内分散写入 100 MiB 后，最终有效对象可能接近
512 MiB。

本优化针对以下场景：

1. 一个文件在一次发布周期中只有一个写者；
2. `fsync` 保证本地 staging 持久化并登记中心元数据；
3. 对象上传由 `flush-staging`、API 或 staging 高水位触发；
4. Put 完成前，其他客户端不会读取该文件；
5. 同一个 64 MiB chunk 内的重复随机写应复用同一个 slice；
6. 未写入区域在本地和对象存储中始终保持为洞。

## 3. 总体架构变化

社区版写路径是：

1. 创建不可变 slice；
2. 写入普通 block；
3. 将 slice 追加到中心元数据；
4. writeback 模式下再异步上传 block；
5. slice 过多时执行 dense compact。

优化后的 sparse 写路径是：

1. 为 `(inode, chunk index)` 创建一个本地 sparse generation；
2. 对齐的 4 KiB 写直接覆盖 `.stage` 文件对应位置；
3. 用 bitmap 记录 data、zero 和 inherit 三种页面状态；
4. `fsync` checkpoint 本地文件，然后幂等登记同一个全局 sparse slice；
5. `flush-staging` 只打包 data 页面并上传 manifest；
6. 发布完成后删除本地 generation；
7. 后续写入创建新的 generation，已发布对象保持不可变。

未启用 `--sparse-chunk-slice` 时，普通 JuiceFS 写入和 writeback 路径保持社区版
行为。早期实现过的 dense `PatchStaging` 方案已经全部删除。

## 4. 配置和格式版本

### 4.1 新增 sparse 格式版本

修改文件：

- `pkg/meta/interface.go`
- `cmd/format.go`
- `cmd/config.go`
- `cmd/mount.go`

社区版最大元数据格式版本是 1。优化版保留新卷默认版本 1，同时把客户端可识别的
最大版本提升到 2：

- `DefaultFormatVersion = 1`
- `MaxVersion = 2`
- `SparseFormatVersion = 2`

启用前必须执行：

```shell
juicefs config META-URL --enable-sparse-chunk
```

该命令把卷的 `MetaVersion` 从 1 升级到 2。升级不可回退。社区版客户端只支持
版本 1，因此会拒绝挂载版本 2，而不是把 sparse slice 当成普通 slice 错误读取。

### 4.2 新增挂载参数

修改文件：

- `cmd/flags.go`
- `cmd/mount.go`
- `pkg/chunk/cached_store.go`

新增参数：

- `--sparse-chunk-slice`：启用 sparse generation；
- `--stage-usage-high-ratio`：staging 数据达到配置 cache 容量的指定比例时发布；
  设置为 `0` 时关闭水位触发。

挂载要求：

```shell
juicefs mount META-URL MOUNTPOINT \
  --writeback \
  --sparse-chunk-slice \
  --stage-usage-high-ratio 0 \
  --cache-dir /path/to/cache
```

该模式要求：

- 已升级到元数据格式 2；
- 启用 writeback；
- 使用磁盘 cache，不能使用纯内存 cache。

原先独立的 `--controlled-staging`、low watermark 和 staging buffer 参数已经
删除。sparse staging 不再依赖早期的 dense staging 覆盖方案。

## 5. Sparse slice 标识

修改文件：

- `pkg/meta/interface.go`
- `pkg/chunk/sparse.go`
- `pkg/vfs/sparse_staging.go`

使用 slice ID 的 bit 62 标识 packed sparse slice：

```text
SparseSliceBit = 1 << 62
```

选择 bit 62 的原因：

- 保持 `int64` 正数，不占用符号位；
- 去掉标志位后仍是正常的全局 slice ID；
- 不修改已有 24 字节 slice 元数据记录格式；
- 删除、读取和 compact 路径可以快速识别对象格式。

`NewSlice` 仍由中心元数据分配普通 ID，本地创建 generation 时再设置 sparse bit。

## 6. 本地 sparse staging

主要新增文件：

- `pkg/vfs/sparse_staging.go`

这是本次优化最大的代码模块，共新增约 792 行。

### 6.1 Generation 索引

每个 generation 用以下键定位：

```text
(inode, 64 MiB chunk index)
```

内存中维护：

- `active`：当前可覆盖写的 generation；
- `pending`：已经冻结、等待或正在发布的 generation；
- `byID`：按 sparse slice ID 查找 generation；
- `localIDs`：读路径快速判断某个全局 slice 是否仍由本地 staging 提供。

同一发布周期内，对一个 chunk 的所有随机写都复用 `active`，不会为每次 4 KiB
覆盖重新创建 slice。

### 6.2 `.stage` 文件布局

每个 generation 对应一个文件：

```text
sparse-staging/<inode>-<chunk>-<slice-id>.stage
```

逻辑布局：

```text
0        8 KiB       16 KiB                         64 MiB + 16 KiB
| slot0 | slot1 | sparse data area                       |
```

数据区逻辑长度为 64 MiB，但通过按偏移写入保持文件系统稀疏。未写页不会分配磁盘
block。

### 6.3 双 header checkpoint

每个 8 KiB header slot 保存：

- magic 和格式版本；
- sequence；
- inode、chunk index 和 sparse slice ID；
-真实 EOF 和 mtime；
- data bitmap 和 zero bitmap；
- active/frozen 状态；
- CRC32。

`fsync` 时执行：

1. `fsync` `.stage` 文件，先持久化数据页；
2. 把新 header 写到另一个 slot；
3. 再次 `fsync` 文件；
4. `fsync` staging 目录。

恢复时读取两个 slot，选择 CRC 正确且 sequence 最大的版本。该实现没有逐写
journal；双 slot 是避免 header 覆盖写过程中掉电损坏上一次 checkpoint 的最小
机制。

## 7. 页面状态和随机写

修改文件：

- `pkg/vfs/sparse_staging.go`
- `pkg/vfs/writer.go`

一个 chunk 包含 16384 个 4 KiB 页面。每个 generation 保存两张 2048 字节的
bitmap：

- data bitmap：页面内容保存在 staging 文件；
- zero bitmap：页面逻辑内容是零，但不保存 4 KiB 零数据；
- 两张 bitmap 都未置位：inherit，继承更老 generation 或已发布 slice。

### 7.1 完整页面覆盖

当写入正好覆盖完整且对齐的 4 KiB 页面时，直接 `WriteAt`，不读取旧数据。

非零页面写入 staging 数据区并设置 data bit。全零页面只设置 zero bit，因此不会
占用数据区空间。

### 7.2 非完整页面写入

当一次写入没有覆盖完整 4 KiB 页面时，必须执行 read-modify-write：

1. 当前 generation 已有 data 页时，从 `.stage` 读取；
2. 当前页是 zero 时，以零页为基础；
3. 当前页是 inherit 时，从旧 slice 和 pending generation 读取基础页；
4. 修改目标字节后重新判断页面是 data 还是 zero。

这是 POSIX 部分写语义所必需的，不能直接覆盖而丢失页面内未修改字节。

### 7.3 Writer 分流

`pkg/vfs/writer.go` 新增 `sparseFileWriter`。启用 sparse 后：

- `Open` 返回 `sparseFileWriter`；
- `Write` 直接进入 sparse staging；
- `Flush`/`Fsync` 调用 `SyncInode`；
- 普通 `fileWriter` 和社区版 slice writer 不参与 sparse 写入；
- 普通模式仍走社区版 writer。

sparse 模式不再启动只服务普通 `fileWriter` 的周期性 flush goroutine。

## 8. fsync 与中心元数据登记

修改文件：

- `pkg/vfs/sparse_staging.go`
- `pkg/meta/base.go`
- `pkg/meta/context.go`
- `pkg/meta/slice.go`
- `pkg/meta/redis.go`
- `pkg/meta/sql.go`
- `pkg/meta/tkv.go`

### 8.1 登记时机

社区版每个新 slice 完成后立即追加元数据。优化版改为：

1. 普通 `write` 只更新本地 staging；
2. `fsync`/flush/close 先完成本地 checkpoint；
3. checkpoint 成功后调用 `WriteEOF` 登记 sparse slice；
4. 第一次登记追加一条记录；
5. 后续 fsync 使用同一个 ID，幂等更新 EOF 和 mtime，不追加 slice。

Put 不再修改 slice 元数据，只上传该 ID 对应的 payload 和 manifest。

### 8.2 为什么单独传递 EOF

中心元数据中的 sparse slice 使用：

```text
Pos  = 0
Off  = 0
Size = 64 MiB
Len  = 64 MiB
```

这是为了覆盖整个 chunk 并表达 inherit/data/zero 页面。如果直接按 `Len` 更新文件
长度，短文件会被错误扩展到 chunk 边界。

因此新增：

- `WriteEOF`
- `WithFileLength`
- `sliceCommitLength`

真实 EOF 通过 context 独立传递，slice 记录本身仍保持社区版格式。

### 8.3 三类元数据引擎幂等

Redis、SQL 和 TKV 都增加相同语义：

1. 在现有 chunk 记录中查找相同 slice ID；
2. ID 不存在时正常追加；
3. ID 已存在且记录完全一致时不再追加；
4. ID 已存在但位置、大小或范围不一致时返回 `EINVAL`；
5. 即使 slice 已存在，仍允许更新 EOF、mtime 和 changelog。

SQL 路径还必须避免重复增加 `sliceRef`，否则每次 fsync 都会抬高引用计数，导致
对象无法正确回收。

Redis 事务把 chunk key 纳入 WATCH，防止检查与追加之间出现并发变化。

## 9. Changelog、dump 和 backup 兼容

修改文件：

- `pkg/meta/changelog.go`
- `pkg/meta/changelog_replay.go`
- `pkg/meta/dump.go`
- `pkg/meta/backup.go`

### 9.1 Changelog

WRITE changelog 从原来的 7 个参数扩展为可选的第 8 个 EOF 参数：

- 旧记录仍按 7 参数解析；
- 新 sparse 记录用第 8 参数恢复真实文件长度；
- replay 检测到 EOF 后调用 `WriteEOF`。

### 9.2 nextChunk 计数器

sparse ID 含 bit 62。dump 和 backup 计算下一个 slice ID 时必须先清除该格式位：

```text
counterID = sparseID &^ SparseSliceBit
```

否则恢复后会把 nextChunk 推进到接近 `1 << 62`，破坏正常 ID 分配。

## 10. 元数据读取和 overlay

修改文件：

- `pkg/meta/base.go`
- `pkg/meta/openfile.go`
- `pkg/meta/interface.go`
- `pkg/vfs/reader.go`

社区版 `buildSlice` 会根据覆盖关系消除旧 slice。该行为不适用于 sparse：
新 generation 中的 inherit 页面必须继续读取更老的数据。

因此新增：

- `OverlaySlice`：保留 slice 在 chunk 中的真实位置；
- `OverlayReader`：返回持久化写入顺序；
- `HybridReader`：普通 chunk 继续返回社区版视图，包含 sparse 时返回 overlay；
- open-file overlay cache：缓存 raw overlay 结果。

读取顺序是：

1. 读取更老的 dense 或 sparse slice；
2. 按写入顺序应用后续 sparse generation；
3. inherit 不修改结果；
4. zero 清零；
5. data 用 packed payload 或本地 `.stage` 覆盖。

本地未发布 generation 的 slice ID 已经存在于中心元数据，但对象 manifest 尚未
存在。读路径通过 `localIDs` 跳过对象读取，再叠加本地 staging，从而保证同节点
read-your-writes。

## 11. Sparse 对象格式

主要新增文件：

- `pkg/chunk/sparse.go`

该文件新增约 431 行。

### 11.1 Manifest

manifest 保存：

- magic、版本和 CRC32；
- page size：4 KiB；
- logical chunk size：64 MiB；
- payload part size：4 MiB；
- data bitmap；
- zero bitmap；
- payload part 数量和每个 part 的实际大小。

manifest 在所有 payload part 上传成功后最后上传。读者只有在 manifest 可见后才
能确定该 generation 已完整发布。

### 11.2 Packed payload

Put 时按页号顺序遍历 data bitmap，只上传 data 页面：

- inherit 页面不上传；
- zero 页面不上传；
- data 页面紧密排列；
- 每个 payload part 最大 4 MiB。

因此对象存储占用接近真实 data 页总量，而不是 slice 的 64 MiB 逻辑长度。

### 11.3 Prefix rank

packed payload 中没有保留页面洞。读取逻辑页时需要计算该页之前有多少 data 页。

manifest 加载后构建按 bitmap 字节的 prefix rank。读取单页时可以快速把逻辑页号
转换为 packed payload 偏移，避免从 bitmap 起点重复扫描。

`ReadSparsePages` 会把同一 payload part 中的页面合并为一次 range GET，避免
4 KiB 随机读为每页发起一个对象请求。

### 11.4 删除

`cachedStore.Remove` 检测 sparse bit 后调用 `RemoveSparse`：

1. 先通过回调删除可能尚未上传的本地 `.stage`；
2. 再按 sparse ID 前缀列举并删除 manifest、payload 和部分上传对象；
3. 清理 manifest 内存缓存。

该顺序避免 inode 已删除后，本地 staging 在重启时重新登记元数据。

## 12. Compact 行为变化

修改文件：

- `pkg/meta/base.go`

社区版 dense compact 不理解 inherit 页面。如果直接 compact sparse slice，会把
内部洞写成零，既破坏继承语义，也重新引入空间放大。

当前行为是：chunk 中只要存在 sparse slice，就跳过社区版 compact。

影响：

- 单次发布周期保持每 chunk 一个 slice；
- 多次 Put 后可能积累多个不可变 sparse generation；
- 当前尚未实现 sparse-aware compact；
- 上层应控制 Put 频率，避免长期产生过多 generation。

## 13. 显式 Put

新增文件：

- `cmd/flush_staging.go`
- `pkg/vfs/flush_staging.go`
- `pkg/vfs/internal.go`

新增命令：

```shell
juicefs flush-staging MOUNTPOINT/path
juicefs flush-staging --wait MOUNTPOINT/path
juicefs flush-staging --wait MOUNTPOINT
```

命令通过 JuiceFS 控制文件向挂载进程发送 `FlushStaging` 消息。

支持：

- 单文件；
- 目录递归；
- 整个挂载点；
- 默认异步发布；
- `--wait` 等待上传完成。

当前命令只发布 sparse generation，不再包含早期 dense writeback staging 的扫描、
定向 block 上传和等待队列。

发布流程：

1. checkpoint 并登记目标 inode 的 active generation；
2. 把 active 标记 frozen 并移入 pending；
3. 上传 packed payload；
4. 最后上传 manifest；
5. 使本地读缓存失效；
6. 删除 pending 和 `.stage`。

上传失败时 pending 和本地文件保留，可以使用相同 ID 重试。

## 14. 发布期间并发写

修改文件：

- `pkg/vfs/sparse_staging.go`

发布开始后，generation 必须保持不可变，否则正在上传的 payload 和 bitmap 可能
不一致。

因此 Put 会冻结 active。发布期间如果继续写同一 chunk，会创建新的 active
generation。旧 generation 保持在 pending，直到上传成功。

这意味着：

- 不发生 Put/写重叠时，每 chunk 通常只有一个 generation；
- Put 与写重叠时会临时产生多个 generation；
- `publishMu` 串行化多个 Put；
- 读锁阻止发布清理正在被本地读使用的 stage 文件。

如果应用能够额外保证 Put 期间完全不写，可以在未来进一步简化 pending 状态机；
当前实现没有依赖该更强假设。

## 15. 重启恢复

修改文件：

- `pkg/vfs/sparse_staging.go`

挂载时扫描 `sparse-staging/*.stage`：

1. 检查文件逻辑长度；
2. 读取两个 header slot；
3. 选择 sequence 最大的有效 checkpoint；
4. 恢复 data/zero bitmap、EOF、mtime 和 frozen 状态；
5. 重建 active、pending、byID 和 localIDs；
6. 使用相同 sparse ID 幂等调用 `WriteEOF`；
7. inode 已删除时清理该 stage；
8. 没有有效 header 的文件视为从未成功 fsync，直接删除。

恢复不会自动 Put。只要使用同一个 cache 目录，本地 fsync 后的数据可以继续读取或
由用户显式发布。

恢复不解决 staging 磁盘损坏或丢失。中心元数据已经登记但对象尚未 Put 时，如果
本地 staging 盘同时丢失，该部分数据无法恢复。

## 16. 删除、硬链接和 rename

修改文件：

- `pkg/chunk/cached_store.go`
- `pkg/chunk/sparse.go`
- `pkg/vfs/sparse_staging.go`

删除沿用社区版 slice 生命周期：

- inode 仍有硬链接时不删除 slice；
- open-unlink 时遵循 sustained inode，最后一个句柄关闭后再清理；
- rename 覆盖导致旧 inode 最终删除时，进入同一 DeleteSlice 链路；
- 未上传 generation 删除本地 `.stage`；
- 已上传或部分上传 generation 删除对象前缀。

删除和 Put 使用同一个 `publishMu`，避免发布线程读取文件时另一个线程删除 stage。

## 17. 高水位发布

修改文件：

- `cmd/flags.go`
- `pkg/chunk/cached_store.go`
- `pkg/vfs/sparse_staging.go`

每次 sparse 写结束后统计所有 active/pending generation 的 data 页面数量：

```text
staged bytes = data page count * 4096
```

当该值达到：

```text
cache-size * stage-usage-high-ratio
```

后台异步执行 `PublishAll`。

设置 `--stage-usage-high-ratio=0` 会完全关闭该机制。关闭后，普通写、idle timer 和
`upload-delay` 都不会自动发布 sparse generation。

## 18. 暂不支持的操作

修改文件：

- `pkg/vfs/vfs.go`

sparse 模式下明确返回 `ENOTSUP`：

- truncate；
- fallocate；
- copy_file_range。

这些操作会改变页面继承关系、EOF 或跨 inode slice 引用。在没有完整 sparse 语义
前显式拒绝，比静默产生错误数据安全。

metadata clone 在 Put 前也不安全：clone 可以共享中心 slice ID，但新 inode 不会
自动共享原 inode 的本地 staging overlay。

## 19. 对社区版原有行为的影响

未启用 sparse 时：

- 默认元数据格式仍是版本 1；
- 普通 writer、writeback、upload-delay 和 dense compact 保持社区版行为；
- 不会创建 `.stage` sparse 文件；
- 不会使用 sparse bit；
- 不会改变现有对象 key；
- 社区版 dump、load 和 changelog 仍可使用原格式。

启用 sparse 后：

- 卷升级为格式 2；
- 每 chunk 使用本地 sparse generation；
- fsync 后元数据可见，但 Put 前远端对象不可见；
- Put 前其他节点读取会因 manifest 不存在而失败；
- dense compact 被禁用；
- 对象使用 packed sparse 格式。

## 20. 测试变更

新增测试文件：

- `pkg/meta/sparse_test.go`
- `pkg/vfs/sparse_staging_test.go`

扩展测试：

- `pkg/chunk/cached_store_test.go`

覆盖内容包括：

- 三类元数据引擎 sparse ID 和 EOF；
- 重复 fsync 不增加 slice；
- 相同 ID 不同记录返回错误；
- dump/backup 计数器清除 sparse bit；
- 完整页覆盖、部分页 RMW、zero 和 inherit；
- 双 header 重启恢复；
- Put 前本机读取；
- Put 后对象读取；
- manifest CRC 和损坏检测；
- 删除未上传 staging；
- open-unlink；
- hardlink 和 rename 覆盖；
- Put/读并发保护；
- 高水位发布；
- 部分上传对象清理。

## 21. 实测结果

测试参数：

- 512 MiB 地址范围；
- 随机读写 100 MiB；
- 4 KiB；
- `direct=0`；
- `psync`；
- 单线程，`iodepth=1`；
- 固定非零数据；
- 读前卸载重挂并清理页缓存；
- 没有使用顺序 `dd` 预热。

精简后的优化版三轮中位数：

- 随机写：25473 IOPS，99.5 MiB/s，p99 64.3 微秒；
- 随机读：4316 IOPS，16.9 MiB/s，p99 610 微秒；
- slice：8 个，对应 512 MiB 范围内的 8 个 chunk；
- staging 实际占用约 100.50 MiB；
- Put 后对象实际占用约 100.07 MiB。

上午同口径单轮基线：

- XFS：写 324051 IOPS，读 4754 IOPS；
- 社区版：写 126 IOPS，读 3388 IOPS；
- 优化版相对社区版：写约 203 倍，读约提升 27%。

XFS 写入结果主要反映 buffered page cache。使用 `fallocate=none` 后，随机写
100 MiB 的 XFS 文件实际占用约 100.70 MiB；fio 默认预分配时会占用完整 512 MiB。

社区版测试结束后，当前元数据最终 compact 为 8 个 slice，仍被引用的有效对象约
511.92 MiB。测试过程中还产生大量历史对象和本地 cache；这些可被 GC 或 cache
淘汰，但最终有效对象仍接近 512 MiB，因为 dense compact 物化了内部洞。

## 22. 已知风险和后续工作

### 22.1 应用时序约束

中心元数据在 fsync 后已经包含 sparse ID，而 manifest 只在 Put 时上传。应用必须
保证 Put 完成前其他客户端不读取。

### 22.2 Staging 单点

未 Put 数据只存在本地 staging。节点和 staging 盘同时丢失会导致数据不可恢复。

### 22.3 多写者

当前没有分布式租约或冲突检测。多个客户端同时写同一文件可能产生最后写入者覆盖。

### 22.4 多次 Put

每次 Put 后再次写入会创建新 generation。当前没有 sparse-aware compact，长期
频繁 Put 会增加 sparse slice 数量。

### 22.5 GC 和统计

`juicefs gc` 尚未完整理解 sparse manifest/payload 引用，空间统计也不完整。
删除单个 sparse slice 已有专用前缀清理逻辑，但全局 GC 仍需扩展。

### 22.6 对象存储能力

sparse payload 当前直接调用对象存储：

- 尚未接入社区版 block compression；
- 尚未接入上传限速；
- 尚未接入 tier 选择；
- manifest cache 目前没有容量上限。

### 22.7 功能范围

truncate、fallocate、copy_file_range 和 Put 前 clone 尚未支持。该功能应保持
实验性，只用于本文约束明确的工作负载。

## 23. 完整文件变更清单

### CLI 和配置

- `cmd/config.go`：增加不可逆 sparse 格式升级；
- `cmd/flags.go`：增加 sparse 和高水位参数；
- `cmd/flush_staging.go`：增加显式发布命令；
- `cmd/format.go`：新卷继续使用格式版本 1；
- `cmd/main.go`：注册 `flush-staging`；
- `cmd/mount.go`：构建 sparse 配置并检查格式版本。

### Chunk 和对象存储

- `pkg/chunk/sparse.go`：manifest、payload、packed read、删除；
- `pkg/chunk/cached_store.go`：sparse 配置、manifest cache、Remove 分流；
- `pkg/chunk/cached_store_test.go`：sparse 对象格式测试。

### 元数据

- `pkg/meta/interface.go`：格式版本、sparse bit、overlay 接口；
- `pkg/meta/context.go`：真实 EOF context；
- `pkg/meta/slice.go`：幂等记录比较、EOF 长度和 changelog 参数；
- `pkg/meta/base.go`：WriteEOF、overlay/hybrid read、跳过 dense compact；
- `pkg/meta/openfile.go`：overlay 缓存；
- `pkg/meta/redis.go`：Redis 幂等登记；
- `pkg/meta/sql.go`：SQL 幂等登记和引用计数保护；
- `pkg/meta/tkv.go`：TKV 幂等登记；
- `pkg/meta/changelog.go`：兼容 7/8 参数 WRITE；
- `pkg/meta/changelog_replay.go`：恢复真实 EOF；
- `pkg/meta/dump.go`：nextChunk 清除 sparse bit；
- `pkg/meta/backup.go`：backup nextChunk 清除 sparse bit；
- `pkg/meta/sparse_test.go`：三引擎和备份兼容测试。

### VFS

- `pkg/vfs/sparse_staging.go`：本地 generation、checkpoint、恢复和发布；
- `pkg/vfs/writer.go`：sparse writer 分流；
- `pkg/vfs/reader.go`：全局和本地 sparse overlay 读取；
- `pkg/vfs/flush_staging.go`：按路径发布 sparse generation；
- `pkg/vfs/internal.go`：控制消息处理；
- `pkg/vfs/vfs.go`：拒绝尚未支持的操作；
- `pkg/vfs/sparse_staging_test.go`：VFS 生命周期和故障语义测试。

### 文档

- `docs/en/guide/cache.md`：英文启用说明和限制；
- `docs/zh_cn/guide/cache.md`：中文启用说明和限制；
- `docs/zh_cn/development/sparse_controlled_staging.md`：完整设计文档；
- `docs/zh_cn/development/sparse_staging_community_diff.md`：本文。
