# Fast Write Linux/FUSE 测试集

本目录提供 Fast Write 的可重复验证入口，测试目标限定为 Linux、FUSE、单写客户端以及
`upload-fast-write --wait` 后发布。

测试分为四层：

1. Go 功能测试：4 KiB/64 MiB 边界、随机模型、checkpoint 后覆盖隔离、graceful restart、
   双 FD mutation 并发、short write/copy、unlink 后越 EOF 写与 fstat、seal/清理故障、
   reflink/bitmap 稀疏复制、seal 中间态恢复、namespace 生命周期、staging 重试和
   metadata engine 兼容。
2. Go micro benchmark：区分 XFS 文件操作、Fast Write 对齐写、非对齐 RMW、并行 inode、
   本地/远端读取和 checkpoint 成本。
3. 真实 FUSE 功能测试：format、mount、fsync、进程崩溃恢复、同步/异步 upload、
   writeback staging、hardlink/rename、跨 chunk 写，以及社区版客户端读取已发布数据。
4. 真实 FUSE 性能测试：在同一底层文件系统上串行比较 XFS、社区版和 Fast Write，
   保存原始 fio JSON、环境信息、每轮 wall time、中位数和相对比例。

## 依赖

- Linux，具备 `/dev/fuse` 和挂载权限；
- Fast Write 本地目录所在文件系统建议为 XFS；
- `fio`、Python 3、`mountpoint`、`findmnt`、`sha256sum`；
- 能编译 JuiceFS 的 Go 工具链。

测试只创建并删除由 `mktemp` 生成的专用目录，不会复用生产卷。FUSE 崩溃恢复用例会
`SIGKILL` 测试挂载进程，但不会匹配或终止其他 JuiceFS 进程。

## 构建当前版和社区基线

```bash
cd /data/github/heo/juicefs
GO_BIN=/tmp/cursor-go/go/bin/go \
  bash tests/fast-write/build-binaries.sh
```

默认把当前工作区编译为 Fast Write 二进制，把当前分支父提交编译为社区基线。脚本使用
`git archive`，不会切换分支或创建额外 worktree。输出目录默认为
`/tmp/juicefs-fast-write-test-bin`，其中的 `binaries.env` 可直接加载：

```bash
source /tmp/juicefs-fast-write-test-bin/binaries.env
```

也可显式指定：

```bash
COMMUNITY_COMMIT=4f20009 \
OUT_DIR=/tmp/fw-bin \
GO_BIN=/path/to/go \
  bash tests/fast-write/build-binaries.sh
```

## Go 功能测试和 benchmark

```bash
MOCKEY_CHECK_GCFLAGS=false /tmp/cursor-go/go/bin/go test \
  ./pkg/vfs -run 'FastWrite' -count=1

MOCKEY_CHECK_GCFLAGS=false /tmp/cursor-go/go/bin/go test -race \
  ./pkg/vfs -run 'FastWrite' -count=1

MOCKEY_CHECK_GCFLAGS=false /tmp/cursor-go/go/bin/go test \
  ./pkg/vfs -run '^$' -bench 'FastWrite' -benchmem -count=5
```

## 真实 FUSE 功能测试

```bash
source /tmp/juicefs-fast-write-test-bin/binaries.env
sudo -E FAST_WRITE_BIN="$FAST_WRITE_BIN" COMMUNITY_BIN="$COMMUNITY_BIN" \
  TEST_BASE=/data KEEP_RESULTS=1 \
  bash tests/fast-write/functional.sh
```

成功时脚本会输出结果目录。该目录包含挂载日志、预期文件 checksum、控制命令输出和
测试环境。功能测试会验证：

- 非对齐、跨页、跨 64 MiB chunk 写和稀疏 EOF；
- `fsync` 后杀死挂载进程，再挂载恢复；
- truncate 缩短被拒绝、punch-hole/写零、普通复制；
- rename、hardlink 和最后链接删除；
- 默认异步 upload 的 seal 边界，以及后续 `--wait`；
- Fast Write 挂载自动补齐 `writeback_cache` 和 `noatime`，测试命令不再显式传入；
- 开关 JuiceFS `--writeback` 时 staging 对象可见后再提交 metadata；
- 社区基线客户端无需理解 Fast Write 本地格式即可读取发布结果；
- FUSE BDI `max_ratio` 与挂载参数一致。

## 性能测试

快速检查测试脚本和挂载环境：

```bash
sudo -E FAST_WRITE_BIN="$FAST_WRITE_BIN" COMMUNITY_BIN="$COMMUNITY_BIN" \
  PROFILE=smoke ROUNDS=1 TEST_BASE=/data KEEP_RESULTS=1 \
  bash tests/fast-write/performance.sh
```

完整矩阵：

```bash
sudo -E FAST_WRITE_BIN="$FAST_WRITE_BIN" COMMUNITY_BIN="$COMMUNITY_BIN" \
  PROFILE=full ROUNDS=3 TEST_BASE=/data KEEP_RESULTS=1 \
  bash tests/fast-write/performance.sh
```

`PROFILE=full` 的每个目标默认执行以下固定负载：

- 单文件、4 KiB、buffered、随机覆盖、总写入 500 MiB；
- 16 文件、16 job、libaio、iodepth=16、buffered、总写入 500 MiB；
- 同一负载的 iodepth=128；
- 16 文件写入后并行逐文件 `fsync`，分别记录写阶段和 fsync 阶段；
- 16 文件、libaio、direct I/O；
- 单文件、1 MiB buffered 顺序写；
- Fast Write 额外记录 512 MiB `upload-fast-write --wait`。

所有目标均位于同一个 `TEST_BASE` 底层文件系统，逐个串行运行；每轮轮换
XFS/community/Fast Write 顺序。社区版与 Fast Write 的 FUSE BDI `max_ratio` 都固定为
`FUSE_MAX_DIRTY_RATIO`（默认 50），避免把 BDI 配额差异误判为实现收益。

主要可调变量：

```text
PROFILE=smoke|full
ROUNDS=3
WORKLOADS=buffered-rand-single,buffered-rand-q16,buffered-rand-q128,...
TARGETS=xfs,community,fast-write
FILE_SIZE=4G
TOTAL_IO_SIZE=500M
FUSE_MAX_DIRTY_RATIO=50
FAST_WRITE_SYNC_CONCURRENCY=16
JUICEFS_WRITEBACK=1
FAST_WRITE_MIN_XFS_PERCENT=0
FAST_WRITE_MIN_COMMUNITY_MULTIPLIER=0
```

最后两个变量默认为只报告、不设性能门槛。设置非零值后，脚本会对
`buffered-rand-q16` 的中位数进行回归判定。

结果目录中的关键文件：

- `environment.txt`：commit、内核、CPU、内存、底层文件系统、fio 和 BDI 参数；
- `metrics.tsv`：每轮 fio、fsync、upload wall time；
- `fio/*.json`：完整 fio 原始结果；
- `summary.json`：机器可读中位数、P99 和相对比例；
- `summary.tsv`：便于人工查看的摘要。

buffered 无 fsync 结果表示前台吸收脏页能力，不表示数据已经持久化；带 fsync 的
`total` 和 `upload-fast-write --wait` 才分别对应本地 checkpoint 和 publish barrier。
