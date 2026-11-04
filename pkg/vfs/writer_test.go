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
	"log"
	"math/rand"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
)

// createTestVFSWriteback mounts a volume with disk writeback staging and a long
// upload delay so overwrites can hit PatchStaging before Put.
func createTestVFSWriteback(t *testing.T) *VFS {
	t.Helper()
	mp := "/jfs"
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mp
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{
		Name:        "test",
		UUID:        uuid.New().String(),
		Storage:     "mem",
		BlockSize:   4096,
		Compression: "none",
		DirStats:    true,
	}
	if err := m.Init(format, true); err != nil {
		log.Fatalf("setting: %s", err)
	}
	conf := &Config{
		Meta:    metaConf,
		Format:  *format,
		Version: "Juicefs",
		Chunk: &chunk.Config{
			BlockSize:              format.BlockSize * 1024,
			Compress:               format.Compression,
			MaxUpload:              2,
			MaxDownload:            200,
			BufferSize:             32 << 20,
			CacheSize:              64 << 20,
			CacheDir:               t.TempDir(),
			CacheChecksum:          chunk.CsNone,
			CacheEviction:          "2-random",
			Writeback:              true,
			WritebackThresholdSize: format.BlockSize*1024 + 1,
			UploadDelay:            time.Hour,
			AutoCreate:             true,
			PutTimeout:             time.Second,
			GetTimeout:             time.Second * 2,
			MaxRetries:             3,
		},
		FuseOpts: &FuseOptions{},
	}
	conf.Chunk.SelfCheck(format.UUID)
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": format.Name}, registry))
	store := chunk.NewCachedStore(blob, *conf.Chunk, registry)
	return NewVFS(conf, m, store, registerer, registry)
}

// Overwrites inside the not-yet-uploaded window of a single chunk should stay
// on one slice instead of appending a new meta slice per write.
func TestWriteCoalesceInMemoryOverwrites(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(1, 1, []uint32{1}))

	entry, fh, err := v.Create(ctx, 1, "coalesce", 0644, 0, syscall.O_RDWR)
	if err != 0 {
		t.Fatalf("create: %s", err)
	}
	inode := entry.Inode

	const size = 1 << 20 // 1 MiB, below default 4 MiB block so nothing is FlushTo'd mid-way
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = 1
	}
	if err := v.Write(ctx, inode, buf, 0, fh); err != 0 {
		t.Fatalf("initial write: %s", err)
	}

	r := rand.New(rand.NewSource(1))
	patch := make([]byte, 4096)
	for i := 0; i < 200; i++ {
		for j := range patch {
			patch[j] = byte(i + 2)
		}
		off := uint64(r.Intn(size - len(patch)))
		if err := v.Write(ctx, inode, patch, off, fh); err != 0 {
			t.Fatalf("overwrite %d at %d: %s", i, off, err)
		}
	}
	if err := v.Flush(ctx, inode, fh, 0); err != 0 {
		t.Fatalf("flush: %s", err)
	}

	var slices []meta.Slice
	if st := v.Meta.Read(ctx, inode, 0, &slices); st != 0 {
		t.Fatalf("read slices: %s", st)
	}
	if len(slices) != 1 {
		t.Fatalf("expected 1 slice after in-memory overwrites, got %d: %+v", len(slices), slices)
	}

	out := make([]byte, size)
	n, err := v.Read(ctx, inode, out, 0, fh)
	if err != 0 || n != size {
		t.Fatalf("read back: n=%d err=%s", n, err)
	}
}

// A write that fully covers an older uncommitted slice should cancel it so it
// never becomes a meta slice.
func TestWriteCancelFullyCoveredPendingSlice(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(1, 1, []uint32{1}))

	entry, fh, err := v.Create(ctx, 1, "cover", 0644, 0, syscall.O_RDWR)
	if err != 0 {
		t.Fatalf("create: %s", err)
	}
	inode := entry.Inode

	fw := v.writer.(*dataWriter).find(inode)
	if fw == nil {
		t.Fatal("fileWriter not found")
	}

	// First small write, then force-freeze that slice so the next overlapping
	// write cannot reuse it and must open a new one.
	p1 := []byte("hello-world-pad-xxxx") // 20 bytes
	if err := v.Write(ctx, inode, p1, 0, fh); err != 0 {
		t.Fatalf("write1: %s", err)
	}
	fw.Lock()
	c := fw.chunks[0]
	if c == nil || len(c.slices) != 1 {
		fw.Unlock()
		t.Fatalf("expected 1 live slice, got %v", c)
	}
	s0 := c.slices[0]
	s0.freezed = true
	go s0.flushData()
	fw.Unlock()

	// Full cover of [0, 20) with a longer write → new slice, old one cancelled.
	p2 := make([]byte, 64)
	for i := range p2 {
		p2[i] = 'Z'
	}
	if err := v.Write(ctx, inode, p2, 0, fh); err != 0 {
		t.Fatalf("write2: %s", err)
	}

	fw.Lock()
	cancelled := 0
	for _, s := range fw.chunks[0].slices {
		if s.cancelled {
			cancelled++
		}
	}
	fw.Unlock()
	if cancelled != 1 {
		t.Fatalf("expected 1 cancelled slice before flush, got %d", cancelled)
	}

	if err := v.Flush(ctx, inode, fh, 0); err != 0 {
		t.Fatalf("flush: %s", err)
	}
	var slices []meta.Slice
	if st := v.Meta.Read(ctx, inode, 0, &slices); st != 0 {
		t.Fatalf("read slices: %s", st)
	}
	if len(slices) != 1 {
		t.Fatalf("expected 1 committed slice after covering write, got %d: %+v", len(slices), slices)
	}
	if slices[0].Len != 64 {
		t.Fatalf("unexpected slice len %d", slices[0].Len)
	}
}

// After flush in writeback mode, data sits in local staging until UploadDelay.
// Overwrites in that window should rewrite staging instead of appending slices.
func TestWriteOverwriteStagingReducesSlices(t *testing.T) {
	v := createTestVFSWriteback(t)
	ctx := NewLogContext(meta.NewContext(1, 1, []uint32{1}))

	entry, fh, err := v.Create(ctx, 1, "stage-ow", 0644, 0, syscall.O_RDWR)
	if err != 0 {
		t.Fatalf("create: %s", err)
	}
	inode := entry.Inode

	const size = 256 << 10
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = 1
	}
	if err := v.Write(ctx, inode, buf, 0, fh); err != 0 {
		t.Fatalf("initial write: %s", err)
	}
	if st := v.Flush(ctx, inode, fh, 0); st != 0 {
		t.Fatalf("flush1: %s", st)
	}

	patch := bytes.Repeat([]byte{7}, 4096)
	for i := 0; i < 50; i++ {
		off := uint64((i * 5003) % (size - len(patch)))
		if st := v.Write(ctx, inode, patch, off, fh); st != 0 {
			t.Fatalf("overwrite %d: %s", i, st)
		}
	}
	if st := v.Flush(ctx, inode, fh, 0); st != 0 {
		t.Fatalf("flush2: %s", st)
	}

	var slices []meta.Slice
	if st := v.Meta.Read(ctx, inode, 0, &slices); st != 0 {
		t.Fatalf("read slices: %s", st)
	}
	if len(slices) != 1 {
		t.Fatalf("expected 1 slice after staging overwrites, got %d: %+v", len(slices), slices)
	}

	out := make([]byte, size)
	n, err := v.Read(ctx, inode, out, 0, fh)
	if err != 0 || n != size {
		t.Fatalf("read back: n=%d err=%s", n, err)
	}
	off := uint64((49 * 5003) % (size - len(patch)))
	for i := 0; i < len(patch); i++ {
		if out[int(off)+i] != 7 {
			t.Fatalf("data mismatch at %d", int(off)+i)
		}
	}
}
