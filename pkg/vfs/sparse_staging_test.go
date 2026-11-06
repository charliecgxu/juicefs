/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vfs

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func createTestVFSSparse(t *testing.T) (*VFS, object.ObjectStorage) {
	t.Helper()
	mp := "/jfs"
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mp
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{
		Name: "sparse-test", UUID: uuid.New().String(), Storage: "mem",
		BlockSize: 4096, Compression: "none", DirStats: true,
	}
	if err := m.Init(format, true); err != nil {
		log.Fatalf("setting: %s", err)
	}
	require.NoError(t, m.NewSession(true))
	t.Cleanup(func() { _ = m.CloseSession() })
	conf := &Config{
		Meta: metaConf, Format: *format, Version: "Juicefs",
		Chunk: &chunk.Config{
			BlockSize: format.BlockSize * 1024, Compress: "none",
			MaxUpload: 2, MaxDownload: 16, BufferSize: 64 << 20,
			CacheSize: 128 << 20, CacheDir: t.TempDir(),
			CacheChecksum: chunk.CsNone, CacheEviction: "2-random",
			Writeback: true, SparseChunkSlice: true,
			WritebackThresholdSize: format.BlockSize*1024 + 1,
			AutoCreate:             true, PutTimeout: time.Second, GetTimeout: time.Second,
			MaxRetries: 3,
		},
		FuseOpts: &FuseOptions{},
	}
	conf.Chunk.SelfCheck(format.UUID)
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": format.Name}, registry))
	store := chunk.NewCachedStore(blob, *conf.Chunk, registry)
	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	return NewVFS(conf, m, store, registerer, registry), blob
}

func createSparseFile(t *testing.T, v *VFS, name string) (Context, meta.Ino, uint64) {
	t.Helper()
	ctx := NewLogContext(meta.NewContext(1, 1, []uint32{1}))
	entry, fh, st := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	return ctx, entry.Inode, fh
}

func TestSparseVisibilityAndEOF(t *testing.T) {
	for _, size := range []int{11, 4 << 20} {
		t.Run(fmt.Sprintf("eof-%d", size), func(t *testing.T) {
			v, _ := createTestVFSSparse(t)
			ctx, inode, fh := createSparseFile(t, v, "eof")
			data := bytes.Repeat([]byte{0x5A}, size)
			require.Zero(t, v.Write(ctx, inode, data, 0, fh))
			require.Zero(t, v.Fsync(ctx, inode, 0, fh))

			var attr meta.Attr
			require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
			require.Equal(t, uint64(size), attr.Length)
			var slices []meta.Slice
			require.Zero(t, v.Meta.Read(ctx, inode, 0, &slices))
			require.Len(t, slices, 1)
			require.True(t, meta.IsSparseSliceID(slices[0].Id))
			require.Equal(t, uint32(meta.ChunkSize), slices[0].Size)
			id := slices[0].Id

			got := make([]byte, size)
			n, st := v.Read(ctx, inode, got, 0, fh)
			require.Zero(t, st)
			require.Equal(t, size, n)
			require.Equal(t, data, got)

			resp, st := v.flushStagingPaths(ctx, nil, false)
			require.Zero(t, st)
			require.Equal(t, uint64(1), resp.SliceCount)
			require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
			require.Equal(t, uint64(size), attr.Length)
			require.Zero(t, v.Meta.Read(ctx, inode, 0, &slices))
			require.Len(t, slices, 1)
			require.Equal(t, id, slices[0].Id)
			require.True(t, meta.IsSparseSliceID(slices[0].Id))
			require.Equal(t, uint32(meta.ChunkSize), slices[0].Len)
			require.Equal(t, uint64(size), attr.Length)
		})
	}
}

func TestSparseRepeatedFsyncReusesSlice(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "repeat-fsync")
	page := bytes.Repeat([]byte{0x5A}, chunk.SparsePageSize)
	require.Zero(t, v.Write(ctx, inode, page, 0, fh))
	require.Zero(t, v.Fsync(ctx, inode, 0, fh))
	id := sparseGenerationID(t, v, ctx, inode)

	off := uint64(8 * chunk.SparsePageSize)
	require.Zero(t, v.Write(ctx, inode, page, off, fh))
	require.Zero(t, v.Fsync(ctx, inode, 0, fh))
	require.Equal(t, id, sparseGenerationID(t, v, ctx, inode))

	var attr meta.Attr
	require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
	require.Equal(t, off+chunk.SparsePageSize, attr.Length)
	require.True(t, v.sparse.hasGeneration(id))
}

func TestSparseSyncKeepsNewestMtime(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "mtime")
	require.Zero(t, v.Write(ctx, inode, []byte("old"), meta.ChunkSize, fh))
	require.Zero(t, v.Write(ctx, inode, []byte("new"), 0, fh))

	oldTime := time.Unix(1700000000, 100)
	newTime := oldTime.Add(time.Second)
	v.sparse.Lock()
	for key, g := range v.sparse.active {
		if key.Inode != inode {
			continue
		}
		if key.Chunk == 0 {
			g.Mtime = newTime.UnixNano()
		} else {
			g.Mtime = oldTime.UnixNano()
		}
	}
	v.sparse.Unlock()
	require.NoError(t, v.sparse.SyncInode(inode))

	var attr meta.Attr
	require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
	require.Equal(t, newTime.Unix(), attr.Mtime)
	require.Equal(t, uint32(newTime.Nanosecond()), attr.Mtimensec)
}

func TestSparseReadGuardBlocksPublishCleanup(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "read-publish")
	require.Zero(t, v.Write(ctx, inode, bytes.Repeat([]byte{7}, chunk.SparsePageSize), 0, fh))
	require.Zero(t, v.Fsync(ctx, inode, 0, fh))
	require.True(t, v.sparse.beginRead(inode, 0))

	done := make(chan error, 1)
	go func() {
		_, err := v.sparse.PublishInodes([]meta.Ino{inode})
		done <- err
	}()
	select {
	case err := <-done:
		require.Failf(t, "publish completed during guarded read", "error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	v.sparse.endRead()
	require.NoError(t, <-done)
}

func putDenseSlice(t *testing.T, v *VFS, ctx meta.Context, inode meta.Ino, data []byte) {
	t.Helper()
	var id uint64
	require.Zero(t, v.Meta.NewSlice(ctx, &id))
	w := v.Store.NewWriter(id, 0)
	w.SetWriteback(false)
	_, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(len(data)))
	require.Zero(t, v.Meta.Write(ctx, inode, 0, 0,
		meta.Slice{Id: id, Size: uint32(len(data)), Len: uint32(len(data))}, time.Now()))
}

func TestSparseRMWAndHoleFallthrough(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "rmw")
	old := bytes.Repeat([]byte{'A'}, 3*chunk.SparsePageSize)
	putDenseSlice(t, v, ctx, inode, old)
	_, fh, st := v.Open(ctx, inode, syscall.O_RDWR)
	require.Zero(t, st)

	require.Zero(t, v.Write(ctx, inode, []byte{'Z'}, chunk.SparsePageSize+123, fh))
	require.Zero(t, v.Fsync(ctx, inode, 0, fh))
	_, st = v.flushStagingPaths(ctx, nil, false)
	require.Zero(t, st)

	got := make([]byte, len(old))
	n, st := v.Read(ctx, inode, got, 0, fh)
	require.Zero(t, st)
	require.Equal(t, len(got), n)
	require.Equal(t, byte('A'), got[0], "untouched sparse hole must inherit old slice")
	require.Equal(t, byte('A'), got[chunk.SparsePageSize+122])
	require.Equal(t, byte('Z'), got[chunk.SparsePageSize+123])
	require.Equal(t, byte('A'), got[chunk.SparsePageSize+124])
	require.Equal(t, old[2*chunk.SparsePageSize:], got[2*chunk.SparsePageSize:])
}

func TestSparseOnePagePhysicalAndOneSlice(t *testing.T) {
	v, blob := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "one-page")
	off := uint64(meta.ChunkSize - chunk.SparsePageSize)
	page := bytes.Repeat([]byte{7}, chunk.SparsePageSize)
	require.Zero(t, v.Write(ctx, inode, page, off, fh))
	require.Zero(t, v.Write(ctx, inode, []byte("patch"), off+100, fh))
	_, st := v.flushStagingPaths(ctx, nil, false)
	require.Zero(t, st)

	var slices []meta.Slice
	require.Zero(t, v.Meta.Read(ctx, inode, 0, &slices))
	require.Len(t, slices, 1, "all writes in a chunk must share one generation")

	var payloadBytes int64
	var manifests int
	objects, _, _, err := blob.List(context.Background(), "chunks/", "", "", "", 100, true)
	require.NoError(t, err)
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key(), ".manifest") {
			manifests++
		} else if strings.Contains(obj.Key(), "_sparse_") {
			payloadBytes += obj.Size()
		}
	}
	require.Equal(t, 1, manifests)
	require.Equal(t, int64(chunk.SparsePageSize), payloadBytes)
}

func TestSparseMultiChunkPublishKeepsRealEOF(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "multi-chunk")
	end := uint64(meta.ChunkSize + chunk.SparsePageSize)
	require.Zero(t, v.Write(ctx, inode, []byte("first"), 0, fh))
	require.Zero(t, v.Write(ctx, inode, []byte("last"), end-4, fh))

	var attr meta.Attr
	require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
	require.Zero(t, attr.Length, "writes without fsync remain local")
	resp, st := v.flushStagingPaths(ctx, nil, false)
	require.Zero(t, st)
	require.Equal(t, uint64(2), resp.SliceCount)
	require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
	require.Equal(t, end, attr.Length)
	for indx := uint32(0); indx < 2; indx++ {
		var slices []meta.Slice
		require.Zero(t, v.Meta.Read(ctx, inode, indx, &slices))
		require.Len(t, slices, 1)
		require.Equal(t, uint32(meta.ChunkSize), slices[0].Len)
	}
}

func TestSparseSameNodeRecoveryDoesNotPut(t *testing.T) {
	v, blob := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "recover")
	require.Zero(t, v.Write(ctx, inode, []byte("recover-me"), 17, fh))
	require.Zero(t, v.Fsync(ctx, inode, 0, fh))

	reader2 := NewDataReader(v.Conf, v.Meta, v.Store)
	recovered, err := newSparseStaging(v.Conf, v.Meta, v.Store, reader2)
	require.NoError(t, err)
	require.GreaterOrEqual(t, recovered.Length(inode), uint64(27))
	buf := make([]byte, chunk.SparsePageSize)
	require.NoError(t, recovered.overlay(inode, 0, 0, buf))
	require.Equal(t, []byte("recover-me"), buf[17:27])

	var attr meta.Attr
	require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
	require.Equal(t, uint64(27), attr.Length)
	objects, _, _, err := blob.List(context.Background(), "chunks/", "", "", "", 100, true)
	require.NoError(t, err)
	for _, obj := range objects {
		require.False(t, strings.Contains(obj.Key(), "_sparse"), "recovery must not auto Put")
	}
}

func TestSparseHighWatermarkPublishes(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	v.Conf.Chunk.StageUsageHigh = 0.000001
	ctx, inode, fh := createSparseFile(t, v, "watermark")
	require.Zero(t, v.Write(ctx, inode, make([]byte, chunk.SparsePageSize), 0, fh))
	time.Sleep(50 * time.Millisecond)
	var attr meta.Attr
	require.Zero(t, v.Meta.GetAttr(ctx, inode, &attr))
	require.Zero(t, attr.Length, "logical zero pages must not consume the staging space watermark")

	require.Zero(t, v.Write(ctx, inode, bytes.Repeat([]byte{9}, chunk.SparsePageSize), 0, fh))
	require.Eventually(t, func() bool {
		return v.Meta.GetAttr(ctx, inode, &attr) == 0 && attr.Length == chunk.SparsePageSize
	}, 5*time.Second, 10*time.Millisecond)
}

func sparseGenerationID(t *testing.T, v *VFS, ctx Context, inode meta.Ino) uint64 {
	t.Helper()
	var slices []meta.Slice
	require.Zero(t, v.Meta.Read(ctx, inode, 0, &slices))
	require.Len(t, slices, 1)
	return slices[0].Id
}

func releaseAndWait(t *testing.T, v *VFS, ctx Context, inode meta.Ino, fh uint64) {
	t.Helper()
	require.Zero(t, v.Meta.Close(ctx, inode))
	v.releaseFileHandle(inode, fh)
}

func TestSparseUnlinkRemovesUnpublishedStage(t *testing.T) {
	v, blob := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "unlink")
	require.Zero(t, v.Write(ctx, inode, bytes.Repeat([]byte{1}, chunk.SparsePageSize), 0, fh))
	require.Zero(t, v.Fsync(ctx, inode, 0, fh))
	id := sparseGenerationID(t, v, ctx, inode)
	require.True(t, v.sparse.hasGeneration(id))

	releaseAndWait(t, v, ctx, inode, fh)
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "unlink"))
	require.Eventually(t, func() bool { return !v.sparse.hasGeneration(id) }, 5*time.Second, 10*time.Millisecond)

	objects, _, _, err := blob.List(context.Background(), "chunks/", "", "", "", 100, true)
	require.NoError(t, err)
	require.Empty(t, objects)
}

func TestSparseOpenUnlinkDefersStageRemoval(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	ctx, inode, fh := createSparseFile(t, v, "open-unlink")
	data := bytes.Repeat([]byte{2}, chunk.SparsePageSize)
	require.Zero(t, v.Write(ctx, inode, data, 0, fh))
	require.Zero(t, v.Fsync(ctx, inode, 0, fh))
	id := sparseGenerationID(t, v, ctx, inode)

	_, readFH, st := v.Open(ctx, inode, syscall.O_RDONLY)
	require.Zero(t, st)
	releaseAndWait(t, v, ctx, inode, fh)
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "open-unlink"))
	require.True(t, v.sparse.hasGeneration(id))
	got := make([]byte, len(data))
	n, st := v.Read(ctx, inode, got, 0, readFH)
	require.Zero(t, st)
	require.Equal(t, len(got), n)
	require.Equal(t, data, got)

	releaseAndWait(t, v, ctx, inode, readFH)
	require.Eventually(t, func() bool { return !v.sparse.hasGeneration(id) }, 5*time.Second, 10*time.Millisecond)
}

func TestSparseHardlinkAndRenameOverwriteLifecycle(t *testing.T) {
	v, _ := createTestVFSSparse(t)
	ctx, first, firstFH := createSparseFile(t, v, "first")
	require.Zero(t, v.Write(ctx, first, bytes.Repeat([]byte{3}, chunk.SparsePageSize), 0, firstFH))
	require.Zero(t, v.Fsync(ctx, first, 0, firstFH))
	firstID := sparseGenerationID(t, v, ctx, first)
	releaseAndWait(t, v, ctx, first, firstFH)

	_, st := v.Link(ctx, first, meta.RootInode, "first-link")
	require.Zero(t, st)
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "first"))
	require.True(t, v.sparse.hasGeneration(firstID))

	_, victim, victimFH := createSparseFile(t, v, "victim")
	require.Zero(t, v.Write(ctx, victim, bytes.Repeat([]byte{4}, chunk.SparsePageSize), 0, victimFH))
	require.Zero(t, v.Fsync(ctx, victim, 0, victimFH))
	victimID := sparseGenerationID(t, v, ctx, victim)
	releaseAndWait(t, v, ctx, victim, victimFH)

	require.Zero(t, v.Rename(ctx, meta.RootInode, "first-link", meta.RootInode, "victim", 0))
	require.True(t, v.sparse.hasGeneration(firstID))
	require.Eventually(t, func() bool { return !v.sparse.hasGeneration(victimID) }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "victim"))
	require.Eventually(t, func() bool { return !v.sparse.hasGeneration(firstID) }, 5*time.Second, 10*time.Millisecond)

	_, err := os.Stat(v.sparse.path(victim, 0, victimID))
	require.True(t, os.IsNotExist(err))
}
