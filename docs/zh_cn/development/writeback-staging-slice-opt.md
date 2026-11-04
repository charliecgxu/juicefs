# Writeback Staging 减 Slice 优化

本文说明 `opt` 分支上的 writeback 覆盖写减碎片优化：设计动机、架构、关键路径、配置、测试方法与实测结果。

> **生效前提**：挂载开启 `--writeback`。非 writeback 行为与原版基本一致。  
> **兼容性**：不改 volume format / `MinClientVersion`；可与旧客户端同卷混挂。减 slice 仅新客户端 + writeback。

---

## 1. 背景与目标

JuiceFS 文件数据按 chunk → slice → block 组织。覆盖写在无法并入现有可写 slice 时，会 **append 新的 meta slice**。writeback 下块先落本地 staging 再异步 Put；若每次覆盖都新开 slice：

- `next_chunk` / slice 数量快速膨胀
- 读路径要拼更多 slice，延迟与放大上升
- compact/GC 压力增大

**目标**：在「块仍在本地 staging、尚未 Put」时可安全改写的前提下，减少因覆盖写产生的新 slice，且随机读写性能不劣化（理想情况明显提升）。

---

## 2. 设计思路

### 2.1 核心判断

| 数据所在位置 | 策略 |
|--------------|------|
| 内存中、尚未 stage 的未上传区间 | 尽量写入**同一** `sliceWriter`（推迟 `FlushTo` + 用真实 `Uploaded()` 水位） |
| 已 commit 到 meta、块仍在 pending staging | **原地补丁**（`PatchStaging`），不新建 meta slice |
| 已上传到对象存储 / 正在 uploading | 回退原逻辑：开新 slice |

### 2.2 为何用内存脏缓冲而不是直接改 staging 文件

早期尝试过覆盖时整块 restage / 对 staging 文件 `pwrite`，随机写场景下 CPU 与磁盘开销过大，甚至写崩。现方案：

1. `PatchStaging` 把补丁写入进程内 `stagingDirty`（按 block 缓存整块镜像）
2. 本机读优先 `readStagingDirty`
3. **上传前** `flushStagingDirty` 再 `WriteAt` 回 staging 文件，然后 Put
4. 若刷盘失败：**保留 dirty，跳过本次上传**，避免把未打补丁的旧文件传到对象存储

### 2.3 配套能力

- **水位上传**：staging 占用 / 盘容量 ≥ high 开始挤上传，降到 low 停止（默认 0.8 / 0.7）
- **`flush-staging` / `flush2`**：按路径或整卷强制上传（控制通道 opcode `FlushStaging=1009`）

---

## 3. 架构与模块

```text
FUSE / VFS.Write
    → fileWriter.Write / writeChunk          (pkg/vfs/writer.go)
        → findWritableSlice (Uploaded 水位)
        → tryOverwriteStaging → PatchStaging (cached_store)
        → cancelFullyCovered
        → sliceWriter.write（writeback 跳过提前 FlushTo）
              ↓ freeze
        → flushData → Finish → stage → pendingKeys
              ↓ delay / 水位 / flush2
        → flushStagingDirty → Put object
```

| 层级 | 文件 | 职责 |
|------|------|------|
| VFS 写 | `pkg/vfs/writer.go` | 推迟 FlushTo、Uploaded 找 slice、cancelFullyCovered、tryOverwriteStaging |
| Chunk 接口 | `pkg/chunk/chunk.go` | `PatchStaging` / `FlushStaging` / `FlushStagingObjects` / `StagingObject` |
| 缓存与上传 | `pkg/chunk/cached_store.go` | dirty 缓冲、读旁路、上传前刷脏、FlushStaging* |
| 磁盘缓存 | `pkg/chunk/disk_cache.go` 等 | 水位驱动上传、force upload |
| 控制面 | `pkg/vfs/flush_staging.go`、`internal.go` | 处理 FlushStaging 消息 |
| CLI | `cmd/flush_staging.go`、`flags.go`、`mount.go` | `flush-staging`/`flush2`、`--stage-usage-*-ratio` |
| Meta | `pkg/meta/interface.go` | 仅新增常量 `FlushStaging = 1009`（控制通道，非引擎 schema） |

**Meta / Object 契约**：

- Meta：打中 patch 时**不为这次覆盖再 `meta.Write` 新 slice**；引擎表结构不变
- Object：仍 Put 同一 block key；首次上传内容已含本地补丁；无新 API

---

## 4. 关键写路径（对比）

### 4.1 覆盖写：旧版

```text
writeChunk → findWritableSlice 常失败（水位按对齐 slen）
  → 新建 sliceWriter + prepareID
  → FlushTo 提前 stage
  → commitThread → meta.Write 再挂一条 Slice
  → next_chunk↑
```

### 4.2 覆盖写：新版

```text
writeChunk
  ├─ findWritableSlice 命中未上传内存区 → 同 slice WriteAt
  │     └─ cancelFullyCovered
  └─ 未命中且无 pending 重叠
        → tryOverwriteStaging
              → meta.Read 定位单条已 commit slice
              → PatchStaging（pending 且非 uploading）
              → stagingDirty + reader.Invalidate
              → 【不】prepareID / 【不】meta.Write
        失败则回退旧「新建 slice」路径
```

### 4.3 读路径

```text
按 meta slices 拼读（不变）
  → 读某 block 时优先 readStagingDirty
  → 否则 bcache / staging 文件 / object.Get（不变）
```

写者本机能立刻读到未上传补丁；他机仍需等该 key 首次 Put（与原 writeback pending 一致）。

---

## 5. 配置与运维

| 参数 / 命令 | 默认 | 说明 |
|-------------|------|------|
| `--writeback` | off | **必须开启**才有减 slice 主收益 |
| `--upload-delay` | `0s` | 代码默认尽快上传；压测可用 `1h` 拉长 staging 窗口 |
| `--stage-usage-high-ratio` | `0.8` | 达该占比开始水位上传；`0` 关闭 |
| `--stage-usage-low-ratio` | `0.7` | 降到该占比停止；须 `< high` |
| `juicefs flush-staging` / `flush2` | — | 参数为**挂载路径**；`--wait` 等待传完 |

示例：

```bash
juicefs mount META_URL /mnt/jfs -d --writeback --upload-delay=1h \
  --cache-dir /var/jfsCache --cache-size 1024

# 强制上传
juicefs flush2 --wait /mnt/jfs
juicefs flush-staging /mnt/jfs/path/to/file
```

---

## 6. 风险与副作用

| 级别 | 说明 |
|------|------|
| 高（writeback 同类） | dirty 在内存：进程 crash 且未刷盘/上传时，**覆盖差量可能丢** |
| 已缓解 | `flushStagingDirty` 失败时保留 dirty 并**跳过 Put**，避免上传旧内容 |
| 中 | 每个被补丁 block 整块进内存直至上传；长 delay + 大量随机覆盖 → RSS 升高 |
| 中 | 推迟 FlushTo → writeback 峰值内存更大 |
| 中 | `cancelFullyCovered` 偏单写友好 |
| 低 | 水位可能带来突发上传；uploading 中无法 patch 则回退新 slice |

---

## 7. 测试方法

### 7.1 单元测试

```bash
export PATH=/tmp/goroot/bin:$PATH   # 或本机 go
go test ./pkg/chunk/ -count=1 -timeout 180s
go test ./pkg/vfs/ -count=1 -timeout 300s
# 聚焦
go test ./pkg/chunk/ -run 'PatchStaging|FlushStaging|Writeback|UploadStaging' -count=1
go test ./pkg/vfs/ -run 'WriteCoalesce|WriteCancel|WriteOverwriteStaging' -count=1
```

### 7.2 功能冒烟（建议）

writeback / 非 writeback 挂载后覆盖：随机覆盖一致性、追加、截断、并发读写、remount 持久化、`flush-staging` CLI。

### 7.3 FIO 对比（优化前后）

```bash
# 两侧均: --writeback --upload-delay=1h
# 预填 1G 文件后：
fio --name=mix --filename=$MNT/fio-1g.dat --size=1G \
  --bs=4k --rw=randrw --rwmixread=50 --ioengine=psync --direct=0 \
  --numjobs=1 --iodepth=1 --time_based=1 --runtime=120 --ramp_time=5 \
  --group_reporting --norandommap --randrepeat=1
```

说明：在严重碎片的原版 writeback 上，对 1G 做满 `io_size=1G` 的 4k 随机混合可能极慢；用 **数据集 1G + 定时 120s** 对比 IOPS/延迟更可行。

---

## 8. 实测结果（摘要）

环境：同机、本地 file 对象存储 + sqlite meta；`--writeback --upload-delay=1h`；fio `randrw` 50/50、`bs=4k`、文件 1G、`runtime=120s`、`direct=0`。

| 指标 | base（原版） | opt（本优化） | opt/base |
|------|-------------|---------------|----------|
| read IOPS | ~7 | ~9406 | ~1335× |
| write IOPS | ~7 | ~9403 | ~1263× |
| read mean lat | ~142 ms | ~72 µs | 大幅下降 |
| write mean lat | ~134 µs | ~32 µs | ~0.24× |
| read p99 | ~2.3 s | ~208 µs | — |

解读：原版在 writeback 长 staging 窗口下 4k 随机覆盖产生大量 slice，读被放大拖死；优化版合并覆盖后读写回到正常量级。

另：`--upload-delay=0` 时 staging 窗口短，减 slice 收益弱于 `1h`，但仍有内存合并与上传积压时的收益。

---

## 9. 变更文件清单

| 路径 | 变更 |
|------|------|
| `pkg/vfs/writer.go` | 写路径核心优化 |
| `pkg/vfs/writer_test.go` | 新增 |
| `pkg/vfs/flush_staging.go` | 新增 |
| `pkg/vfs/internal.go` | FlushStaging 控制处理 |
| `pkg/chunk/cached_store.go` 等 | PatchStaging、dirty、水位、Flush* |
| `pkg/chunk/*_test.go` | 单测 |
| `pkg/meta/interface.go` | `FlushStaging=1009` |
| `cmd/flush_staging.go` | 新增 CLI |
| `cmd/flags.go` / `mount.go` / `main.go` | 参数与命令注册 |

---

## 10. 相关代码入口速查

- `chunkWriter.findWritableSlice` / `sliceWriter.write` / `fileWriter.tryOverwriteStaging` / `cancelFullyCovered`
- `cachedStore.PatchStaging` / `patchStagingDirty` / `readStagingDirty` / `flushStagingDirty`
- `cachedStore.FlushStaging` / `FlushStagingObjects`
- `cmdFlushStaging`（`flush-staging` / `flush2`）
