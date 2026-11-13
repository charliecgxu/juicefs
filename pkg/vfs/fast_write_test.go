//go:build linux
// +build linux

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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/stretchr/testify/require"
)

func fastWriteTestRemote(data []byte) fastWriteRemoteReader {
	return func(off uint64, dst []byte) (int, syscall.Errno) {
		if off >= uint64(len(data)) {
			clear(dst)
			return 0, 0
		}
		n := copy(dst, data[off:])
		clear(dst[n:])
		return n, 0
	}
}

func TestDecodeUploadFastWriteRequest(t *testing.T) {
	encode := func(paths []string, background bool) *utils.Buffer {
		size := 5
		for _, path := range paths {
			size += 4 + len(path)
		}
		request := utils.NewBuffer(uint32(size))
		request.Put32(uint32(len(paths)))
		for _, path := range paths {
			request.Put32(uint32(len(path)))
			request.Put([]byte(path))
		}
		if background {
			request.Put8(1)
		} else {
			request.Put8(0)
		}
		return utils.ReadBuffer(request.Bytes())
	}

	expected := []string{"plain", "dir/line\nbreak", " leading", "trailing "}
	paths, background, ok := decodeUploadFastWriteRequest(encode(expected, true))
	require.True(t, ok)
	require.True(t, background)
	require.Equal(t, expected, paths)

	paths, background, ok = decodeUploadFastWriteRequest(encode(nil, false))
	require.True(t, ok)
	require.False(t, background)
	require.Empty(t, paths)

	for _, malformed := range [][]byte{
		nil,
		{0, 0, 0, 1, 0},
		{0, 0, 0, 1, 0, 0, 0, 0, 0},
		{0, 0, 0, 1, 0, 0, 0, 2, 'x', 0},
		{0, 0, 0, 0, 0, 0},
	} {
		_, _, ok = decodeUploadFastWriteRequest(utils.ReadBuffer(malformed))
		require.False(t, ok, "accepted malformed request %v", malformed)
	}
}

func TestVFSFastWriteRejectsInternalNamespaceMutations(t *testing.T) {
	v := &VFS{fastWrite: &fastWrite{}}
	ctx := meta.NewContext(1, 0, nil)
	for _, command := range []uint32{meta.Rmr, meta.Clone} {
		var response bytes.Buffer
		v.handleInternalMsg(ctx, command, utils.ReadBuffer(nil), &response)
		require.Equal(t, []byte{uint8(syscall.EOPNOTSUPP)}, response.Bytes())
	}
}

type recordingFastWriteStagingStore struct {
	chunk.ChunkStore
	calls  atomic.Int32
	start  chan struct{}
	block  chan struct{}
	failAt int32
}

func (s *recordingFastWriteStagingStore) FlushStagingSlice(id uint64, size int) error {
	call := s.calls.Add(1)
	if s.start != nil {
		select {
		case s.start <- struct{}{}:
		default:
		}
	}
	if s.block != nil {
		<-s.block
	}
	if call == s.failAt {
		return errors.New("injected Fast Write staging flush failure")
	}
	flusher, ok := s.ChunkStore.(interface {
		FlushStagingSlice(uint64, int) error
	})
	if !ok {
		return errors.New("wrapped store cannot flush staging")
	}
	return flusher.FlushStagingSlice(id, size)
}

type failingFastWriteStore struct {
	chunk.ChunkStore
	failAt   int32
	finishes atomic.Int32
	mu       sync.Mutex
	ids      []uint64
}

func (s *failingFastWriteStore) NewWriter(id uint64, tierID uint8) chunk.Writer {
	s.mu.Lock()
	s.ids = append(s.ids, id)
	s.mu.Unlock()
	return &failingFastWriteWriter{Writer: s.ChunkStore.NewWriter(id, tierID), store: s}
}

func (s *failingFastWriteStore) sliceIDs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.ids...)
}

type failingFastWriteWriter struct {
	chunk.Writer
	store *failingFastWriteStore
}

func (w *failingFastWriteWriter) Finish(length int) error {
	if w.store.finishes.Add(1) == w.store.failAt {
		return errors.New("injected Fast Write upload failure")
	}
	return w.Writer.Finish(length)
}

type failingFastWriteMeta struct {
	meta.Meta
	failAt int32
	writes atomic.Int32
}

func (m *failingFastWriteMeta) Write(ctx meta.Context, inode meta.Ino, index uint32, off uint32,
	slice meta.Slice, mtime time.Time) syscall.Errno {
	if m.writes.Add(1) == m.failAt {
		return syscall.EIO
	}
	return m.Meta.Write(ctx, inode, index, off, slice, mtime)
}

type orderingFastWriteMeta struct {
	meta.Meta
	mtimeFlushed atomic.Bool
}

func (m *orderingFastWriteMeta) SetAttr(ctx meta.Context, inode meta.Ino, set uint16, sggidclearmode uint8,
	attr *meta.Attr) syscall.Errno {
	errno := m.Meta.SetAttr(ctx, inode, set, sggidclearmode, attr)
	if errno == 0 && set&meta.SetAttrMtimeNow != 0 {
		m.mtimeFlushed.Store(true)
	}
	return errno
}

type checkingFastWriteLinkMeta struct {
	meta.Meta
	beforeLink func()
}

func (m *checkingFastWriteLinkMeta) Link(ctx meta.Context, inode, parent meta.Ino, name string,
	attr *meta.Attr) syscall.Errno {
	if m.beforeLink != nil {
		m.beforeLink()
	}
	return m.Meta.Link(ctx, inode, parent, name, attr)
}

type checkingFastWriteDataReader struct {
	DataReader
	invalidate func(Ino, uint64, uint64)
}

func (r *checkingFastWriteDataReader) Invalidate(inode Ino, off, length uint64) {
	if r.invalidate != nil {
		r.invalidate(inode, off, length)
	}
	r.DataReader.Invalidate(inode, off, length)
}

type blockingFastWriteLifecycleMeta struct {
	meta.Meta
	lookupStarted  chan struct{}
	continueLookup chan struct{}
	linkCalled     chan struct{}
	lookupOnce     sync.Once
	linkOnce       sync.Once
}

func (m *blockingFastWriteLifecycleMeta) Lookup(ctx meta.Context, parent meta.Ino, name string, inode *meta.Ino,
	attr *meta.Attr, checkPerm bool) syscall.Errno {
	errno := m.Meta.Lookup(ctx, parent, name, inode, attr, checkPerm)
	if name == "fast-write-lifecycle-race" {
		m.lookupOnce.Do(func() { close(m.lookupStarted) })
		<-m.continueLookup
	}
	return errno
}

func (m *blockingFastWriteLifecycleMeta) Link(ctx meta.Context, inode, parent meta.Ino, name string,
	attr *meta.Attr) syscall.Errno {
	m.linkOnce.Do(func() { close(m.linkCalled) })
	return m.Meta.Link(ctx, inode, parent, name, attr)
}

type fastWriteFormatMeta struct {
	meta.Meta
	format meta.Format
}

func (m *fastWriteFormatMeta) GetFormat() meta.Format {
	return m.format
}

func TestFastWriteConfigJSON(t *testing.T) {
	input := []byte(`{
		"FastWriteDir": "/var/lib/juicefs/fast-write",
		"FastWriteSyncConcurrency": 16
	}`)
	var config Config
	require.NoError(t, json.Unmarshal(input, &config))
	require.Equal(t, "/var/lib/juicefs/fast-write", config.FastWriteDir)
	require.Equal(t, 16, config.FastWriteSyncConcurrency)

	output, err := json.Marshal(config)
	require.NoError(t, err)
	require.Contains(t, string(output), `"FastWriteDir"`)
}

func TestFastWriteRecoversV1Fixture(t *testing.T) {
	const (
		fixtureChunkSize  = 64 << 20
		fixturePageSize   = 4096
		fixtureFooterSize = 4096
	)
	root := t.TempDir()
	lock := make([]byte, 64)
	copy(lock[:8], []byte("JFSFWR1\x00"))
	binary.LittleEndian.PutUint32(lock[8:12], 1)
	require.NoError(t, os.WriteFile(filepath.Join(root, ".lock"), lock, 0600))

	generationDir := filepath.Join(root, "inodes", "77", "active-00000000000000000001")
	require.NoError(t, os.MkdirAll(generationDir, 0700))
	chunkPath := filepath.Join(generationDir, "chunk-00000000.data")
	chunk, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	require.NoError(t, err)
	require.NoError(t, chunk.Truncate(fixtureChunkSize+fixtureFooterSize))
	localPage := bytes.Repeat([]byte{'L'}, fixturePageSize)
	_, err = chunk.WriteAt(localPage, fixturePageSize)
	require.NoError(t, err)

	footer := make([]byte, fixtureFooterSize)
	footer[0] = 1 << 1
	copy(footer[2048:2056], []byte("JFSFWC1\x00"))
	binary.LittleEndian.PutUint32(footer[2056:2060], 1)
	binary.LittleEndian.PutUint64(footer[2064:2072], 1)
	binary.LittleEndian.PutUint32(footer[2072:2076], 0)
	binary.LittleEndian.PutUint64(footer[2080:2088], 2*fixturePageSize)
	binary.LittleEndian.PutUint64(footer[2088:2096], 1700000000000000000)
	binary.LittleEndian.PutUint32(footer[4092:4096], crc32.ChecksumIEEE(footer[:4092]))
	_, err = chunk.WriteAt(footer, fixtureChunkSize)
	require.NoError(t, err)
	require.NoError(t, chunk.Close())

	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()

	recovered := fastWrite.get(77)
	require.NotNil(t, recovered)
	require.Equal(t, uint64(2*fixturePageSize), recovered.length)
	require.Len(t, recovered.generations, 1)
	require.Equal(t, int64(1700000000000000000), recovered.generations[0].CheckpointNS)

	remotePage := bytes.Repeat([]byte{'R'}, fixturePageSize)
	got := make([]byte, 2*fixturePageSize)
	n, errno := fastWrite.Read(77, 0, got, fastWriteTestRemote(remotePage))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, remotePage, got[:fixturePageSize])
	require.Equal(t, localPage, got[fixturePageSize:])
}

func TestFastWriteReadWriteAndRecovery(t *testing.T) {
	root := t.TempDir()
	base := append(bytes.Repeat([]byte{'a'}, int(fastWritePageSize)), bytes.Repeat([]byte{'b'}, int(fastWritePageSize))...)
	remote := fastWriteTestRemote(base)

	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.Equal(t, uint32(16384), fastWritePagesPerChunk)
	require.Equal(t, uint32(2048), fastWriteBitmapSize)

	require.Zero(t, fastWrite.Write(10, uint64(len(base)), 123, []byte("fast-write"), remote))
	zeroPage := make([]byte, fastWritePageSize)
	require.Zero(t, fastWrite.Write(10, uint64(len(base)), fastWritePageSize, zeroPage, remote))

	got := make([]byte, len(base))
	n, errno := fastWrite.Read(10, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	expected := append([]byte(nil), base...)
	copy(expected[123:], "fast-write")
	clear(expected[fastWritePageSize : 2*fastWritePageSize])
	require.Equal(t, expected, got)

	require.Zero(t, fastWrite.Checkpoint(10))
	require.NoError(t, fastWrite.close())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	got = make([]byte, len(base))
	n, errno = fastWrite.Read(10, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, expected, got)

	lockData, err := os.ReadFile(filepath.Join(root, ".lock"))
	require.NoError(t, err)
	require.Equal(t, fastWriteLockMagic[:], lockData[:8])
	require.Equal(t, uint32(fastWriteFormatVersion), binary.LittleEndian.Uint32(lockData[8:12]))
	chunkPath := filepath.Join(root, "inodes", "10", generationDirName(fastWriteActive, 1), "chunk-00000000.data")
	info, err := os.Stat(chunkPath)
	require.NoError(t, err)
	require.Equal(t, int64(fastWriteChunkFileSize), info.Size())
	_, _, _, err = readChunkFooter(chunkPath, 1, 0)
	require.NoError(t, err)
}

func TestFastWriteGenerationFallback(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	remote := fastWriteTestRemote(bytes.Repeat([]byte{'r'}, int(fastWritePageSize*2)))

	require.Zero(t, fastWrite.Write(11, fastWritePageSize*2, 0, bytes.Repeat([]byte{'1'}, int(fastWritePageSize)), remote))
	sealed, errno := fastWrite.Seal(11)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), sealed)

	require.Zero(t, fastWrite.Write(11, fastWritePageSize*2, fastWritePageSize+17, []byte("new"), remote))
	got := make([]byte, fastWritePageSize*2)
	n, errno := fastWrite.Read(11, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, bytes.Repeat([]byte{'1'}, int(fastWritePageSize)), got[:fastWritePageSize])
	require.Equal(t, []byte("new"), got[fastWritePageSize+17:fastWritePageSize+20])
	require.Equal(t, byte('r'), got[fastWritePageSize])
}

func TestFastWriteRejectsSecondOwner(t *testing.T) {
	root := t.TempDir()
	first, err := openFastWrite(root)
	require.NoError(t, err)
	defer first.close()

	second, err := openFastWrite(root)
	require.Error(t, err)
	require.Nil(t, second)
}

func TestVFSReportsFastWriteInitializationFailure(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	root := t.TempDir()
	owner, err := openFastWrite(root, v.Conf.Format.UUID)
	require.NoError(t, err)
	defer owner.close()

	conf := *v.Conf
	conf.FastWriteDir = root
	failed := NewVFS(&conf, v.Meta, v.Store, nil, nil)
	require.Error(t, failed.FastWriteInitError())
	require.Nil(t, failed.fastWrite)
}

func TestFastWriteRejectsDifferentVolume(t *testing.T) {
	root := t.TempDir()
	first, err := openFastWrite(root, "volume-a")
	require.NoError(t, err)
	require.Zero(t, first.Write(14, 0, 0, []byte("data"), fastWriteTestRemote(nil)))
	require.Zero(t, first.Checkpoint(14))
	require.NoError(t, first.close())

	second, err := openFastWrite(root, "volume-b")
	require.Error(t, err)
	require.Nil(t, second)
}

func TestFastWriteRejectsUnknownRecoveryEntries(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "inode root", path: filepath.Join("inodes", "unexpected")},
		{name: "inode directory", path: filepath.Join("inodes", "88", "unexpected")},
		{name: "generation directory", path: filepath.Join("inodes", "88", "sealed-00000000000000000001", "unexpected")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			fastWrite, err := openFastWrite(root)
			require.NoError(t, err)
			require.NoError(t, fastWrite.close())
			path := filepath.Join(root, test.path)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, []byte("unknown"), 0600))

			reopened, err := openFastWrite(root)
			require.Error(t, err)
			require.Nil(t, reopened)
		})
	}
}

func TestFastWriteRejectsInvalidLockHeaderSize(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.NoError(t, fastWrite.close())
	lock, err := os.OpenFile(filepath.Join(root, ".lock"), os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, err = lock.Write([]byte{0})
	require.NoError(t, err)
	require.NoError(t, lock.Close())

	reopened, err := openFastWrite(root)
	require.ErrorContains(t, err, "header size")
	require.Nil(t, reopened)
}

func TestFastWriteRejectsNonCanonicalNames(t *testing.T) {
	_, _, ok := parseGenerationDir("active-1")
	require.False(t, ok)
	_, ok = parseChunkName("chunk-1.data")
	require.False(t, ok)
	_, ok = parseChunkName("chunk-0000000A.data")
	require.False(t, ok)

	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.NoError(t, fastWrite.close())
	require.NoError(t, os.Mkdir(filepath.Join(root, "inodes", "088"), 0700))
	reopened, err := openFastWrite(root)
	require.ErrorContains(t, err, "invalid Fast Write inode directory")
	require.Nil(t, reopened)
}

func TestFastWriteRejectsDuplicateGeneration(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.NoError(t, fastWrite.close())
	inodeDir := filepath.Join(root, "inodes", "88")
	require.NoError(t, os.MkdirAll(filepath.Join(inodeDir, "active-00000000000000000001"), 0700))
	require.NoError(t, os.MkdirAll(filepath.Join(inodeDir, "sealed-00000000000000000001"), 0700))
	require.NoError(t, os.WriteFile(
		filepath.Join(inodeDir, "sealed-00000000000000000001", chunkFileName(0)), nil, 0600))

	reopened, err := openFastWrite(root)
	require.ErrorContains(t, err, "duplicate Fast Write generation")
	require.Nil(t, reopened)
}

func TestFastWriteRecoversEmptyPublishedClone(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.NoError(t, fastWrite.close())
	inodeDir := filepath.Join(root, "inodes", "92")
	require.NoError(t, os.MkdirAll(filepath.Join(inodeDir, generationDirName(fastWriteActive, 1)), 0700))
	sealed := filepath.Join(inodeDir, generationDirName(fastWriteSealed, 1))
	require.NoError(t, os.Mkdir(sealed, 0700))

	reopened, err := openFastWrite(root)
	require.NoError(t, err)
	defer reopened.close()
	_, err = os.Stat(sealed)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func stageFastWriteCloneForTest(t *testing.T, fastWrite *fastWrite, inode Ino, state string) string {
	t.Helper()
	file := fastWrite.get(inode)
	require.NotNil(t, file)
	file.Lock()
	defer file.Unlock()
	active := activeGeneration(file)
	require.NotNil(t, active)
	target := fastWrite.transientGenerationDir(inode, state, active.ID)
	require.NoError(t, os.Mkdir(target, 0700))
	previousHook := fastWrite.cloneRangeHook
	defer func() { fastWrite.cloneRangeHook = previousHook }()
	fastWrite.cloneRangeHook = func(_, _ int, _, _, _ uint64) error {
		return syscall.EOPNOTSUPP
	}
	require.NoError(t, fastWrite.cloneGenerationLocked(file, active, target))
	return target
}

func TestFastWriteRecoversInterruptedCloneSeal(t *testing.T) {
	t.Run("sealing clone is discarded", func(t *testing.T) {
		root := t.TempDir()
		fastWrite, err := openFastWrite(root)
		require.NoError(t, err)
		content := bytes.Repeat([]byte{'s'}, int(fastWritePageSize))
		require.Zero(t, fastWrite.Write(88, fastWritePageSize, 0, content, fastWriteTestRemote(nil)))
		require.Zero(t, fastWrite.Checkpoint(88))
		sealing := stageFastWriteCloneForTest(t, fastWrite, 88, fastWriteSealing)
		require.NoError(t, fastWrite.abort())

		fastWrite, err = openFastWrite(root)
		require.NoError(t, err)
		defer fastWrite.close()
		_, err = os.Stat(sealing)
		require.ErrorIs(t, err, os.ErrNotExist)
		got := make([]byte, len(content))
		n, errno := fastWrite.Read(88, 0, got, fastWriteTestRemote(nil))
		require.Zero(t, errno)
		require.Equal(t, len(got), n)
		require.Equal(t, content, got)
	})

	t.Run("published clone rolls back to active", func(t *testing.T) {
		root := t.TempDir()
		fastWrite, err := openFastWrite(root)
		require.NoError(t, err)
		content := bytes.Repeat([]byte{'p'}, int(fastWritePageSize))
		require.Zero(t, fastWrite.Write(89, fastWritePageSize, 0, content, fastWriteTestRemote(nil)))
		require.Zero(t, fastWrite.Checkpoint(89))
		sealed := stageFastWriteCloneForTest(t, fastWrite, 89, fastWriteSealed)
		require.NoError(t, fastWrite.abort())

		fastWrite, err = openFastWrite(root)
		require.NoError(t, err)
		defer fastWrite.close()
		_, err = os.Stat(sealed)
		require.ErrorIs(t, err, os.ErrNotExist)
		_, err = os.Stat(fastWrite.generationDir(89, fastWriteActive, 1))
		require.NoError(t, err)
		got := make([]byte, len(content))
		n, errno := fastWrite.Read(89, 0, got, fastWriteTestRemote(nil))
		require.Zero(t, errno)
		require.Equal(t, len(got), n)
		require.Equal(t, content, got)
	})

	t.Run("retired source completes published seal", func(t *testing.T) {
		root := t.TempDir()
		fastWrite, err := openFastWrite(root)
		require.NoError(t, err)
		content := bytes.Repeat([]byte{'r'}, int(fastWritePageSize))
		require.Zero(t, fastWrite.Write(90, fastWritePageSize, 0, content, fastWriteTestRemote(nil)))
		require.Zero(t, fastWrite.Checkpoint(90))
		stageFastWriteCloneForTest(t, fastWrite, 90, fastWriteSealed)
		active := fastWrite.generationDir(90, fastWriteActive, 1)
		retired := fastWrite.transientGenerationDir(90, fastWriteRetired, 1)
		require.NoError(t, os.Rename(active, retired))
		require.NoError(t, fastWrite.abort())

		fastWrite, err = openFastWrite(root)
		require.NoError(t, err)
		defer fastWrite.close()
		_, err = os.Stat(retired)
		require.ErrorIs(t, err, os.ErrNotExist)
		_, err = os.Stat(fastWrite.generationDir(90, fastWriteSealed, 1))
		require.NoError(t, err)
		_, err = os.Stat(fastWrite.generationDir(90, fastWriteActive, 2))
		require.NoError(t, err)
		got := make([]byte, len(content))
		n, errno := fastWrite.Read(90, 0, got, fastWriteTestRemote(nil))
		require.Zero(t, errno)
		require.Equal(t, len(got), n)
		require.Equal(t, content, got)
	})
}

func TestFastWriteRejectsMismatchedPublishedClone(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	content := bytes.Repeat([]byte{'a'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(91, fastWritePageSize, 0, content, fastWriteTestRemote(nil)))
	require.Zero(t, fastWrite.Checkpoint(91))
	sealed := stageFastWriteCloneForTest(t, fastWrite, 91, fastWriteSealed)
	chunk, err := os.OpenFile(filepath.Join(sealed, chunkFileName(0)), os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, writeFullAt(chunk, []byte{'b'}, 0))
	require.NoError(t, chunk.Close())
	require.NoError(t, fastWrite.abort())

	reopened, err := openFastWrite(root)
	require.ErrorContains(t, err, "not a completed seal clone")
	require.Nil(t, reopened)
}

func TestFastWriteTruncateAndZeroRange(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	base := append(bytes.Repeat([]byte{'a'}, int(fastWritePageSize)), bytes.Repeat([]byte{'b'}, int(fastWritePageSize*2))...)
	remote := fastWriteTestRemote(base)

	newLength := fastWritePageSize + 100
	require.Zero(t, fastWrite.Truncate(12, uint64(len(base)), newLength, remote))
	require.Zero(t, fastWrite.Truncate(12, uint64(len(base)), uint64(len(base)), remote))

	got := make([]byte, len(base))
	n, errno := fastWrite.Read(12, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, base[:newLength], got[:newLength])
	require.Equal(t, make([]byte, uint64(len(base))-newLength), got[newLength:])

	require.Zero(t, fastWrite.ZeroRange(12, uint64(len(base)), 17, 29, true, remote))
	n, errno = fastWrite.Read(12, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, make([]byte, 29), got[17:46])
	require.Equal(t, byte('a'), got[16])
	require.Equal(t, byte('a'), got[46])
}

func TestFastWriteDeletedRecoveryWaitsForMetadataReconciliation(t *testing.T) {
	root := t.TempDir()
	remote := fastWriteTestRemote(nil)
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.Zero(t, fastWrite.Write(13, 0, 0, []byte("data"), remote))
	require.Zero(t, fastWrite.Checkpoint(13))
	require.NoError(t, fastWrite.markDeleted(13))
	require.NoError(t, fastWrite.abort())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	require.True(t, fastWrite.isDeleted(13))
	_, err = os.Stat(filepath.Join(root, "inodes", "13"))
	require.NoError(t, err)
}

func TestFastWriteCopyRangeIsIndependent(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	remote := fastWriteTestRemote(nil)
	source := append(bytes.Repeat([]byte{'x'}, int(fastWritePageSize)), bytes.Repeat([]byte{'y'}, int(fastWritePageSize))...)
	require.Zero(t, fastWrite.Write(20, 0, 0, source, remote))
	require.Zero(t, fastWrite.Checkpoint(20))

	copied, errno := fastWrite.CopyRange(20, 21, 0, 0, 0, 0, uint64(len(source)), remote, remote)
	require.Zero(t, errno)
	require.Equal(t, uint64(len(source)), copied)
	require.Zero(t, fastWrite.Checkpoint(21))

	require.Zero(t, fastWrite.Write(20, uint64(len(source)), 0, bytes.Repeat([]byte{'z'}, int(fastWritePageSize)), remote))
	got := make([]byte, len(source))
	n, errno := fastWrite.Read(21, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, source, got)
}

func TestVFSFastWriteUpload(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(100, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-upload", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	content := append(bytes.Repeat([]byte{'a'}, int(fastWritePageSize)), bytes.Repeat([]byte{'b'}, 777)...)
	require.Zero(t, v.Write(ctx, entry.Inode, content, 0, fh))
	require.Zero(t, v.Flush(ctx, entry.Inode, fh, 0))

	var before []meta.Slice
	require.Zero(t, v.Meta.Read(ctx, entry.Inode, 0, &before))
	require.NotEmpty(t, before)
	for _, slice := range before {
		require.Zero(t, slice.Id)
	}

	stat := &UploadFastWriteResponse{}
	require.Zero(t, v.uploadFastWriteInode(ctx, entry.Inode, stat))
	require.Equal(t, uint64(1), stat.FileCount)
	require.NotZero(t, stat.SliceCount)

	var after []meta.Slice
	require.Zero(t, v.Meta.Read(ctx, entry.Inode, 0, &after))
	require.NotEmpty(t, after)
	attr := &Attr{}
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, attr))
	require.Equal(t, uint64(len(content)), attr.Length)

	got := make([]byte, len(content))
	n, errno := v.Read(ctx, entry.Inode, got, 0, fh)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, content, got)
}

func TestVFSFastWriteUploadSkipsCleanFiles(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(113, 0, []uint32{0}))

	entry, _, errno := v.Create(ctx, meta.RootInode, "fast-write-clean-upload", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	stat := &UploadFastWriteResponse{}
	require.Zero(t, v.uploadFastWriteInode(ctx, entry.Inode, stat))
	require.Zero(t, stat.FileCount)
	require.Zero(t, stat.GenerationCount)
}

func TestVFSFastWriteAsyncUploadSealsBeforeReturn(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	v.Conf.Chunk.Writeback = true
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(block) }) }
	defer release()
	store := &recordingFastWriteStagingStore{ChunkStore: v.Store, start: started, block: block}
	v.Store = store
	ctx := NewLogContext(meta.NewContext(104, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-async-seal", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, bytes.Repeat([]byte{'a'}, int(fastWritePageSize)), 0, fh))
	second, secondFH, errno := v.Create(ctx, meta.RootInode, "fast-write-async-seal-second", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, second.Inode, bytes.Repeat([]byte{'b'}, int(fastWritePageSize)), 0, secondFH))

	stat, errno := v.uploadFastWritePaths(ctx, nil, true)
	require.Zero(t, errno)
	require.Equal(t, "scheduled", stat.State)
	require.Equal(t, uint64(2), stat.FileCount)

	for _, inode := range []Ino{entry.Inode, second.Inode} {
		file := fastWrite.get(inode)
		require.NotNil(t, file)
		file.RLock()
		require.Len(t, file.generations, 2)
		require.Equal(t, fastWriteSealed, file.generations[0].State)
		require.Equal(t, fastWriteActive, file.generations[1].State)
		file.RUnlock()
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("background upload did not reach staging")
	}
	var pending []meta.Slice
	require.Zero(t, v.Meta.Read(ctx, entry.Inode, 0, &pending))
	for _, slice := range pending {
		require.Zero(t, slice.Id, "metadata was published before staging became durable")
	}
	const (
		newMtime     = int64(1800000000)
		newMtimensec = uint32(987654321)
	)
	_, errno = v.SetAttr(ctx, entry.Inode, int(meta.SetAttrMtime), fh, 0, 0, 0, 0,
		newMtime, 0, newMtimensec, 0)
	require.Zero(t, errno)
	release()
	require.Eventually(t, func() bool {
		return len(fastWrite.listInodes()) == 0
	}, 5*time.Second, 10*time.Millisecond)
	var attr Attr
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &attr))
	require.Equal(t, newMtime, attr.Mtime)
	require.Equal(t, newMtimensec, attr.Mtimensec)
}

func TestVFSFastWriteUploadControlRequiresMountOwnerOrRoot(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	uid := uint32(os.Getuid()) + 1
	if uid == 0 {
		uid++
	}
	ctx := NewLogContext(meta.NewContext(109, uid, []uint32{uid}))

	stat, errno := v.uploadFastWritePaths(ctx, nil, false)
	require.Equal(t, syscall.EPERM, errno)
	require.Equal(t, "failed", stat.State)
}

func TestVFSFastWriteInvalidatesReaderBeforeDeletingGeneration(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(105, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-invalidate-order", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	content := bytes.Repeat([]byte{'i'}, int(fastWritePageSize))
	require.Zero(t, v.Write(ctx, entry.Inode, content, 0, fh))

	sealedPath := fastWrite.generationDir(entry.Inode, fastWriteSealed, 1)
	var invalidations atomic.Int32
	v.reader = &checkingFastWriteDataReader{
		DataReader: v.reader,
		invalidate: func(inode Ino, _, _ uint64) {
			if inode != entry.Inode {
				return
			}
			invalidations.Add(1)
			_, statErr := os.Stat(sealedPath)
			require.NoError(t, statErr)
			got := make([]byte, len(content))
			n, readErrno := fastWrite.Read(inode, 0, got, fastWriteTestRemote(nil))
			require.Zero(t, readErrno)
			require.Equal(t, len(got), n)
			require.Equal(t, content, got)
		},
	}

	stat := &UploadFastWriteResponse{}
	require.Zero(t, v.uploadFastWriteInode(ctx, entry.Inode, stat))
	require.Equal(t, int32(1), invalidations.Load())
	_, err = os.Stat(sealedPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestVFSFastWriteUploadPreservesExplicitMtime(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(106, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-explicit-mtime", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, bytes.Repeat([]byte{'m'}, int(fastWritePageSize)), 0, fh))
	const (
		mtime     = int64(1700000000)
		mtimensec = uint32(123456789)
	)
	_, errno = v.SetAttr(ctx, entry.Inode, int(meta.SetAttrMtime), fh, 0, 0, 0, 0, mtime, 0, mtimensec, 0)
	require.Zero(t, errno)
	require.Zero(t, v.uploadFastWriteInode(ctx, entry.Inode, &UploadFastWriteResponse{}))

	var attr Attr
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &attr))
	require.Equal(t, mtime, attr.Mtime)
	require.Equal(t, mtimensec, attr.Mtimensec)
}

func TestVFSFastWriteWritebackFlushesBeforeRelease(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	v.Conf.Chunk.Writeback = true
	store := &recordingFastWriteStagingStore{ChunkStore: v.Store}
	v.Store = store
	ctx := NewLogContext(meta.NewContext(103, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-writeback", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, bytes.Repeat([]byte{'w'}, int(fastWritePageSize)), 0, fh))

	stat := &UploadFastWriteResponse{}
	require.Zero(t, v.uploadFastWriteInode(ctx, entry.Inode, stat))
	require.Positive(t, store.calls.Load())
	require.Empty(t, fastWrite.listInodes())
}

func TestVFSFastWriteLengthAndTruncateSemantics(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(101, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-length", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	content := bytes.Repeat([]byte{'x'}, int(fastWritePageSize))
	require.Zero(t, v.Write(ctx, entry.Inode, content, fastWritePageSize, fh))

	var remote Attr
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &remote))
	require.Equal(t, fastWritePageSize*2, remote.Length)

	var attr Attr
	require.Equal(t, syscall.EOPNOTSUPP, v.Truncate(ctx, entry.Inode, int64(fastWritePageSize), fh, &attr))
	require.Zero(t, v.Truncate(ctx, entry.Inode, int64(fastWritePageSize*3), fh, &attr))
	require.Equal(t, fastWritePageSize*3, attr.Length)
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &remote))
	require.Equal(t, fastWritePageSize*3, remote.Length)

	require.Zero(t, v.Fallocate(ctx, entry.Inode, 0, int64(fastWritePageSize*3), int64(fastWritePageSize), fh))
	length, ok := fastWrite.Length(entry.Inode)
	require.True(t, ok)
	require.Equal(t, fastWritePageSize*4, length)
	got := make([]byte, fastWritePageSize)
	n, errno := v.Read(ctx, entry.Inode, got, fastWritePageSize*3, fh)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, make([]byte, fastWritePageSize), got)
}

func TestVFSFastWriteReportsPartialWriteAndCommittedLength(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(112, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-partial-count", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	fastWrite.writeFileHook = func(file *os.File, data []byte, off int64) (int, error) {
		written, writeErr := writeFullAtCount(file, data[:fastWritePageSize], off)
		if writeErr != nil {
			return written, writeErr
		}
		return written, syscall.ENOSPC
	}
	data := bytes.Repeat([]byte{'p'}, int(fastWritePageSize*2))
	written, errno := v.WriteWithCount(ctx, entry.Inode, data, 0, fh)
	require.Equal(t, uint32(fastWritePageSize), written)
	require.Equal(t, syscall.ENOSPC, errno)
	var attr Attr
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &attr))
	require.Equal(t, fastWritePageSize, attr.Length)
	length, ok := fastWrite.Length(entry.Inode)
	require.True(t, ok)
	require.Equal(t, fastWritePageSize, length)
}

func TestVFSFastWriteRejectsUnsupportedFallocateModes(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(107, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-fallocate-flags", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Equal(t, syscall.EOPNOTSUPP, v.Fallocate(ctx, entry.Inode, 0x04, 0, int64(fastWritePageSize), fh))
	require.Equal(t, syscall.EINVAL, v.Fallocate(ctx, entry.Inode, 0x02, 0, int64(fastWritePageSize), fh))
}

func TestVFSFastWriteOTmpfileWriteLinkAndDiscard(t *testing.T) {
	v, _ := createTestVFS(func(conf *meta.Config) { conf.NoBGJob = true }, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	require.NoError(t, v.Meta.NewSession(true))
	defer v.Meta.CloseSession()
	ctx := NewLogContext(meta.NewContext(124, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "", 0644, 0, syscall.O_RDWR|O_TMPFILE)
	require.Zero(t, errno)
	require.NotNil(t, entry)
	require.NotZero(t, fh)
	var attr Attr
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &attr))
	require.Equal(t, uint32(0), attr.Nlink)
	var entries []*meta.Entry
	require.Zero(t, v.Meta.Readdir(ctx, meta.RootInode, 1, &entries))
	for _, child := range entries {
		require.NotContains(t, string(child.Name), "tmpfile_")
	}

	data := []byte("tmpfile-data")
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &attr), "before write")
	require.Zero(t, v.Write(ctx, entry.Inode, data, 0, fh))
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &attr), "after write")
	got := make([]byte, len(data))
	n, errno := v.Read(ctx, entry.Inode, got, 0, fh)
	require.Zero(t, errno)
	require.Equal(t, data, got[:n])
	require.False(t, fastWrite.isDeleted(entry.Inode))
	require.Equal(t, uint64(len(data)), mustGetAttr(t, v, ctx, entry.Inode).Length)

	_, errno = v.Link(ctx, entry.Inode, meta.RootInode, "fast-write-tmpfile")
	require.Zero(t, errno)
	_, pending := v.fastWritePending.Load(entry.Inode)
	require.False(t, pending)
	v.Release(ctx, entry.Inode, fh)
	require.NotNil(t, fastWrite.get(entry.Inode))
	linked := mustGetAttr(t, v, ctx, entry.Inode)
	require.Equal(t, uint32(1), linked.Nlink)
	opened, openFH, errno := v.Open(ctx, entry.Inode, syscall.O_RDONLY)
	require.Zero(t, errno)
	require.Equal(t, entry.Inode, opened.Inode)
	got = make([]byte, len(data))
	n, errno = v.Read(ctx, entry.Inode, got, 0, openFH)
	require.Zero(t, errno)
	require.Equal(t, data, got[:n])
	v.Release(ctx, entry.Inode, openFH)

	discarded, discardFH, errno := v.Create(ctx, meta.RootInode, "", 0644, 0, syscall.O_RDWR|O_TMPFILE)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, discarded.Inode, data, 0, discardFH))
	require.NotNil(t, fastWrite.get(discarded.Inode))
	v.Release(ctx, discarded.Inode, discardFH)
	require.Eventually(t, func() bool {
		_, stillPending := v.fastWritePending.Load(discarded.Inode)
		return fastWrite.get(discarded.Inode) == nil && !stillPending
	}, 5*time.Second, 10*time.Millisecond)
}

func mustGetAttr(t *testing.T, v *VFS, ctx Context, ino Ino) Attr {
	t.Helper()
	var attr Attr
	require.Zero(t, v.Meta.GetAttr(ctx, ino, &attr))
	return attr
}

func TestVFSFastWriteFailsClosedIfTrashIsEnabledAfterMount(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	ctx := NewLogContext(meta.NewContext(110, 0, []uint32{0}))

	entry, _, errno := v.Create(ctx, meta.RootInode, "fast-write-trash-guard", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	format := v.Meta.GetFormat()
	format.TrashDays = 1
	v.Meta = &fastWriteFormatMeta{Meta: v.Meta, format: format}

	require.Equal(t, syscall.EOPNOTSUPP, v.Unlink(ctx, meta.RootInode, "fast-write-trash-guard"))
	require.Equal(t, syscall.EOPNOTSUPP,
		v.Rename(ctx, meta.RootInode, "fast-write-trash-guard", meta.RootInode, "renamed", 0))
	var inode Ino
	var attr Attr
	require.Zero(t, v.Meta.Lookup(ctx, meta.RootInode, "fast-write-trash-guard", &inode, &attr, true))
	require.Equal(t, entry.Inode, inode)
}

func TestVFSFastWriteTrashReloadSerializesWithLifecycleMutation(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	ctx := NewLogContext(meta.NewContext(111, 0, []uint32{0}))

	entry, _, errno := v.Create(ctx, meta.RootInode, "fast-write-trash-reload", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)

	v.fastWriteLifecycleMu.Lock()
	blocked := make(chan struct{})
	go func() {
		v.BlockFastWriteForTrash()
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("trash reload bypassed lifecycle mutation lock")
	case <-time.After(50 * time.Millisecond):
	}
	v.fastWriteLifecycleMu.Unlock()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("trash reload did not acquire lifecycle mutation lock")
	}

	require.Equal(t, syscall.EOPNOTSUPP, v.Unlink(ctx, meta.RootInode, "fast-write-trash-reload"))
	var inode Ino
	var attr Attr
	require.Zero(t, v.Meta.Lookup(ctx, meta.RootInode, "fast-write-trash-reload", &inode, &attr, true))
	require.Equal(t, entry.Inode, inode)
}

func TestVFSFastWriteDoesNotSerializeUnrelatedNamespaceOperations(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	ctx := NewLogContext(meta.NewContext(113, 0, []uint32{0}))

	runWhileLifecycleLocked := func(name string, operation func() syscall.Errno) {
		t.Helper()
		v.fastWriteLifecycleMu.Lock()
		result := make(chan syscall.Errno, 1)
		go func() { result <- operation() }()
		select {
		case errno := <-result:
			v.fastWriteLifecycleMu.Unlock()
			require.Zero(t, errno, name)
		case <-time.After(time.Second):
			v.fastWriteLifecycleMu.Unlock()
			t.Fatalf("%s waited for the Fast Write lifecycle lock", name)
		}
	}

	runWhileLifecycleLocked("mknod", func() syscall.Errno {
		_, errno := v.Mknod(ctx, meta.RootInode, "fast-write-mknod-unlocked",
			syscall.S_IFREG|0644, 0, 0)
		return errno
	})
	runWhileLifecycleLocked("mkdir", func() syscall.Errno {
		_, errno := v.Mkdir(ctx, meta.RootInode, "fast-write-mkdir-unlocked", 0755, 0)
		return errno
	})
	runWhileLifecycleLocked("symlink", func() syscall.Errno {
		_, errno := v.Symlink(ctx, "target", meta.RootInode, "fast-write-symlink-unlocked")
		return errno
	})
	runWhileLifecycleLocked("rmdir", func() syscall.Errno {
		return v.Rmdir(ctx, meta.RootInode, "fast-write-mkdir-unlocked")
	})

	var created Ino
	var fh uint64
	runWhileLifecycleLocked("create", func() syscall.Errno {
		entry, handle, errno := v.Create(ctx, meta.RootInode, "fast-write-create-unlocked", 0644, 0, syscall.O_RDWR)
		if errno == 0 {
			created, fh = entry.Inode, handle
		}
		return errno
	})
	require.Zero(t, v.Meta.Close(ctx, created))
	v.releaseFileHandle(created, fh)
}

func TestVFSFastWriteSerializesLifecycleLookupAndMutation(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(108, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-lifecycle-race", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, []byte("local"), 0, fh))

	blockingMeta := &blockingFastWriteLifecycleMeta{
		Meta:           v.Meta,
		lookupStarted:  make(chan struct{}),
		continueLookup: make(chan struct{}),
		linkCalled:     make(chan struct{}),
	}
	v.Meta = blockingMeta
	unlinkResult := make(chan syscall.Errno, 1)
	go func() {
		unlinkResult <- v.Unlink(ctx, meta.RootInode, "fast-write-lifecycle-race")
	}()
	select {
	case <-blockingMeta.lookupStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("unlink did not reach lookup")
	}

	linkResult := make(chan syscall.Errno, 1)
	go func() {
		_, linkErrno := v.Link(ctx, entry.Inode, meta.RootInode, "fast-write-lifecycle-link")
		linkResult <- linkErrno
	}()
	select {
	case <-blockingMeta.linkCalled:
		t.Fatal("link entered metadata while unlink lookup/mutation was in progress")
	case <-time.After(100 * time.Millisecond):
	}
	close(blockingMeta.continueLookup)
	require.Zero(t, <-unlinkResult)
	select {
	case <-blockingMeta.linkCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("link did not proceed after unlink completed")
	}
	require.Equal(t, syscall.ENOENT, <-linkResult)
	require.True(t, fastWrite.isDeleted(entry.Inode))
}

func TestVFSFastWriteOnlyFsyncCheckpoints(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(102, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-fsync", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, bytes.Repeat([]byte{'f'}, int(fastWritePageSize)), 0, fh))
	require.Zero(t, v.Flush(ctx, entry.Inode, fh, 0))

	path := filepath.Join(root, "inodes", fmt.Sprint(entry.Inode), generationDirName(fastWriteActive, 1), "chunk-00000000.data")
	chunk, err := os.Open(path)
	require.NoError(t, err)
	footer := make([]byte, fastWriteFooterSize)
	require.NoError(t, readExactAt(chunk, footer, int64(meta.ChunkSize)))
	require.NoError(t, chunk.Close())
	require.True(t, allZero(footer))

	require.Zero(t, v.Fsync(ctx, entry.Inode, 0, fh))
	chunk, err = os.Open(path)
	require.NoError(t, err)
	require.NoError(t, readExactAt(chunk, footer, int64(meta.ChunkSize)))
	require.NoError(t, chunk.Close())
	require.False(t, allZero(footer))
}

func TestVFSFastWriteUploadRetriesFromSealedGeneration(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = root
	v.Conf.FuseOpts.EnableWriteback = true
	store := &failingFastWriteStore{ChunkStore: v.Store, failAt: 2}
	v.Store = store
	ctx := NewLogContext(meta.NewContext(110, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-upload-retry", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	first := bytes.Repeat([]byte{'a'}, int(fastWritePageSize))
	third := bytes.Repeat([]byte{'c'}, int(fastWritePageSize))
	require.Zero(t, v.Write(ctx, entry.Inode, first, 0, fh))
	require.Zero(t, v.Write(ctx, entry.Inode, third, fastWritePageSize*2, fh))

	require.Equal(t, syscall.EIO, v.uploadFastWriteInode(ctx, entry.Inode, &UploadFastWriteResponse{}))
	sealed := fastWrite.generationDir(entry.Inode, fastWriteSealed, 1)
	entries, err := os.ReadDir(sealed)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "chunk-00000000.data", entries[0].Name())

	countUploadedSlices := func() int {
		var slices []meta.Slice
		require.Zero(t, v.Meta.Read(ctx, entry.Inode, 0, &slices))
		count := 0
		for _, slice := range slices {
			if slice.Id != 0 {
				count++
			}
		}
		return count
	}
	require.Zero(t, countUploadedSlices(), "object upload failure published metadata")

	failingMeta := &failingFastWriteMeta{Meta: v.Meta, failAt: 2}
	v.Meta = failingMeta
	require.Equal(t, syscall.EIO, v.uploadFastWriteInode(ctx, entry.Inode, &UploadFastWriteResponse{}))
	require.Equal(t, 1, countUploadedSlices())
	require.Zero(t, v.uploadFastWriteInode(ctx, entry.Inode, &UploadFastWriteResponse{}))
	require.Equal(t, 2, countUploadedSlices())
	ids := store.sliceIDs()
	require.Len(t, ids, 6)
	unique := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		unique[id] = struct{}{}
	}
	require.Len(t, unique, len(ids))
	require.Empty(t, fastWrite.listInodes())

	got := make([]byte, fastWritePageSize*3)
	n, errno := v.Read(ctx, entry.Inode, got, 0, fh)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, first, got[:fastWritePageSize])
	require.True(t, allZero(got[fastWritePageSize:fastWritePageSize*2]))
	require.Equal(t, third, got[fastWritePageSize*2:])
}

func TestFastWriteCrashKeepsLastFlushedGeneration(t *testing.T) {
	root := t.TempDir()
	remote := fastWriteTestRemote(nil)
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	oldPage := bytes.Repeat([]byte{'o'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(31, 0, 0, oldPage, remote))
	require.Zero(t, fastWrite.Checkpoint(31))
	require.Zero(t, fastWrite.Write(31, fastWritePageSize, fastWritePageSize, bytes.Repeat([]byte{'n'}, int(fastWritePageSize)), remote))
	// Simulate a process crash: release the directory lock without flushing the
	// newly added page.
	require.NoError(t, fastWrite.close())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	got := make([]byte, fastWritePageSize*2)
	n, errno := fastWrite.Read(31, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, int(fastWritePageSize), n)
	require.Equal(t, oldPage, got[:fastWritePageSize])
}

func TestFastWriteDropsUncheckpointedOnlyGeneration(t *testing.T) {
	root := t.TempDir()
	remoteData := bytes.Repeat([]byte{'r'}, int(fastWritePageSize*2))
	remote := fastWriteTestRemote(remoteData)
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.Zero(t, fastWrite.Write(32, uint64(len(remoteData)), 123, []byte("not-synced"), remote))
	require.NoError(t, fastWrite.close())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	require.Nil(t, fastWrite.get(32))
	got := make([]byte, len(remoteData))
	n, errno := fastWrite.Read(32, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, remoteData, got)
}

func TestFastWriteUploadBoundaryKeepsNewWrites(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	remote := fastWriteTestRemote(nil)
	require.Zero(t, fastWrite.Write(33, 0, 0, []byte("generation-one"), remote))
	require.Zero(t, fastWrite.Checkpoint(33))
	target, errno := fastWrite.Seal(33)
	require.Zero(t, errno)
	require.Zero(t, fastWrite.Write(33, uint64(len("generation-one")), 0, []byte("generation-two"), remote))

	first, errno := fastWrite.prepareUpload(33, target)
	require.Zero(t, errno)
	require.NotNil(t, first)
	require.Equal(t, target, first.ID)
	require.Zero(t, fastWrite.finishUpload(33, first.ID))
	next, errno := fastWrite.prepareUpload(33, target)
	require.Zero(t, errno)
	require.Nil(t, next)
	file := fastWrite.get(33)
	file.RLock()
	require.Len(t, file.generations, 1)
	require.Equal(t, fastWriteActive, file.generations[0].State)
	file.RUnlock()

	got := make([]byte, len("generation-two"))
	n, errno := fastWrite.Read(33, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, "generation-two", string(got))
}

func TestFastWriteRejectsCorruptFooter(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.Zero(t, fastWrite.Write(32, 0, 0, []byte("data"), fastWriteTestRemote(nil)))
	require.Zero(t, fastWrite.Checkpoint(32))
	require.NoError(t, fastWrite.close())

	path := filepath.Join(root, "inodes", "32", generationDirName(fastWriteActive, 1), "chunk-00000000.data")
	data, err := os.OpenFile(path, os.O_RDWR, 0600)
	require.NoError(t, err)
	_, err = data.WriteAt([]byte{0xff}, int64(meta.ChunkSize))
	require.NoError(t, err)
	require.NoError(t, data.Close())
	fastWrite, err = openFastWrite(root)
	require.Error(t, err)
	require.Nil(t, fastWrite)
}

func TestFastWriteCheckpointDoesNotSealAndReusesFD(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	remote := fastWriteTestRemote(nil)
	data := bytes.Repeat([]byte{'p'}, int(fastWritePageSize*3))
	require.Zero(t, fastWrite.Write(34, 0, 0, data, remote))
	require.Len(t, fastWrite.fds, 1)
	require.Zero(t, fastWrite.Checkpoint(34))
	require.Len(t, fastWrite.fds, 1)
	_, err = os.Stat(filepath.Join(root, "inodes", "34", generationDirName(fastWriteActive, 1)))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "inodes", "34", generationDirName(fastWriteSealed, 1)))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestFastWriteCheckpointOrdersDataFooterAndDirectories(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	require.Zero(t, fastWrite.Write(37, 0, 0, bytes.Repeat([]byte{'o'}, int(fastWritePageSize)), fastWriteTestRemote(nil)))

	var mu sync.Mutex
	var footerEmptyAtSync []bool
	fastWrite.syncFileHook = func(file *os.File) error {
		footer := make([]byte, fastWriteFooterSize)
		if err := readExactAt(file, footer, int64(meta.ChunkSize)); err != nil {
			return err
		}
		mu.Lock()
		footerEmptyAtSync = append(footerEmptyAtSync, allZero(footer))
		mu.Unlock()
		return nil
	}
	var syncedDirs []string
	fastWrite.syncDirHook = func(path string) error {
		syncedDirs = append(syncedDirs, path)
		return nil
	}

	require.Zero(t, fastWrite.Checkpoint(37))
	require.Equal(t, []bool{true, false}, footerEmptyAtSync)
	require.Equal(t, []string{
		fastWrite.generationDir(37, fastWriteActive, 1),
		fastWrite.inodeDir(37),
		filepath.Join(root, "inodes"),
	}, syncedDirs)
}

func TestFastWriteCheckpointRetainsDirtyStateOnDirectorySyncFailure(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	require.Zero(t, fastWrite.Write(38, 0, 0, bytes.Repeat([]byte{'d'}, int(fastWritePageSize)), fastWriteTestRemote(nil)))

	expected := errors.New("directory sync failed")
	fastWrite.syncDirHook = func(path string) error {
		if path == fastWrite.inodeDir(38) {
			return expected
		}
		return nil
	}
	require.Equal(t, syscall.EIO, fastWrite.Checkpoint(38))
	file := fastWrite.get(38)
	require.NotNil(t, file)
	file.RLock()
	require.True(t, file.checkpointPending)
	file.RUnlock()
	require.NotEmpty(t, fastWrite.dirtyChunks)

	fastWrite.syncDirHook = nil
	require.Zero(t, fastWrite.Checkpoint(38))
	file.RLock()
	require.False(t, file.checkpointPending)
	file.RUnlock()
	require.Empty(t, fastWrite.dirtyChunks)
}

func TestFastWriteRecoveredLengthGrowsToRemoteLength(t *testing.T) {
	root := t.TempDir()
	localPage := bytes.Repeat([]byte{'l'}, int(fastWritePageSize))
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.Zero(t, fastWrite.Write(39, fastWritePageSize, 0, localPage, fastWriteTestRemote(nil)))
	require.Zero(t, fastWrite.Checkpoint(39))
	require.NoError(t, fastWrite.close())

	remote := append(bytes.Repeat([]byte{'r'}, int(fastWritePageSize)), bytes.Repeat([]byte{'g'}, int(fastWritePageSize))...)
	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	fastWrite.growRemoteLength(39, uint64(len(remote)))
	got := make([]byte, len(remote))
	n, errno := fastWrite.Read(39, 0, got, fastWriteTestRemote(remote))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, localPage, got[:fastWritePageSize])
	require.Equal(t, remote[fastWritePageSize:], got[fastWritePageSize:])
}

func TestFastWriteSyncConcurrency(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	require.Equal(t, DefaultFastWriteSyncConcurrency, cap(fastWrite.syncSlots))

	const limit = 2
	const tasks = 8
	fastWrite.setSyncConcurrency(limit)
	var active atomic.Int32
	var peak atomic.Int32
	started := make(chan struct{}, tasks)
	release := make(chan struct{})
	errs := make(chan error, tasks)
	var wg sync.WaitGroup
	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- fastWrite.withSyncSlot(func() error {
				current := active.Add(1)
				for previous := peak.Load(); current > previous; previous = peak.Load() {
					if peak.CompareAndSwap(previous, current) {
						break
					}
				}
				started <- struct{}{}
				<-release
				active.Add(-1)
				return nil
			})
		}()
	}
	for i := 0; i < limit; i++ {
		<-started
	}
	close(release)
	wg.Wait()
	close(errs)
	for err = range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(limit), peak.Load())

	expected := errors.New("sync failure")
	require.ErrorIs(t, fastWrite.withSyncSlot(func() error { return expected }), expected)
	require.NoError(t, fastWrite.withSyncSlot(func() error { return nil }))
	fastWrite.setSyncConcurrency(0)
	require.Equal(t, DefaultFastWriteSyncConcurrency, cap(fastWrite.syncSlots))
}

func TestFastWriteCheckpointOnlySyncsDirtyChunks(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	remote := fastWriteTestRemote(nil)
	page := bytes.Repeat([]byte{'d'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(35, meta.ChunkSize*2, 0, page, remote))
	require.Zero(t, fastWrite.Write(35, meta.ChunkSize*2, meta.ChunkSize, page, remote))
	require.Zero(t, fastWrite.Checkpoint(35))

	footer := func(state string, generation uint64, index uint32) []byte {
		path := filepath.Join(root, "inodes", "35", generationDirName(state, generation),
			fmt.Sprintf("chunk-%08x.data", index))
		data, openErr := os.Open(path)
		require.NoError(t, openErr)
		defer data.Close()
		got := make([]byte, fastWriteFooterSize)
		require.NoError(t, readExactAt(data, got, int64(meta.ChunkSize)))
		return got
	}
	firstFooter := footer(fastWriteActive, 1, 0)
	secondFooter := footer(fastWriteActive, 1, 1)
	require.Zero(t, fastWrite.Write(35, meta.ChunkSize*2, fastWritePageSize, page, remote))
	require.Zero(t, fastWrite.Checkpoint(35))
	require.Equal(t, firstFooter, footer(fastWriteSealed, 1, 0))
	require.Equal(t, secondFooter, footer(fastWriteSealed, 1, 1))
	require.NotEqual(t, firstFooter, footer(fastWriteActive, 2, 0))
	_, err = os.Stat(filepath.Join(root, "inodes", "35", generationDirName(fastWriteActive, 2), "chunk-00000001.data"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestFastWriteCheckpointTracksEvictedDirtyChunk(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	fastWrite.fdLimit = 1
	remote := fastWriteTestRemote(nil)
	first := bytes.Repeat([]byte{'a'}, int(fastWritePageSize))
	second := bytes.Repeat([]byte{'b'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(36, meta.ChunkSize*2, 0, first, remote))
	require.Zero(t, fastWrite.Write(36, meta.ChunkSize*2, meta.ChunkSize, second, remote))
	require.Len(t, fastWrite.dirtyFD, 1)
	require.Len(t, fastWrite.dirtyChunks, 2)
	require.Zero(t, fastWrite.Checkpoint(36))
	require.Empty(t, fastWrite.dirtyFD)
	require.Empty(t, fastWrite.dirtyChunks)
	require.NoError(t, fastWrite.close())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	got := make([]byte, fastWritePageSize)
	n, errno := fastWrite.Read(36, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, first, got)
	n, errno = fastWrite.Read(36, meta.ChunkSize, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, second, got)
}

func TestFastWritePartialWriteFailureKeepsCompletedBatchDirty(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	remote := fastWriteTestRemote(nil)
	fastWrite.writeFileHook = func(file *os.File, data []byte, off int64) (int, error) {
		written, writeErr := writeFullAtCount(file, data[:fastWritePageSize], off)
		if writeErr != nil {
			return written, writeErr
		}
		return written, syscall.ENOSPC
	}
	data := bytes.Repeat([]byte{'p'}, int(fastWritePageSize*2))
	guard, errno := fastWrite.beginWrite(41, fastWritePageSize*2, 0, uint64(len(data)))
	require.Zero(t, errno)
	written, errno := fastWrite.writeLocked(guard, 0, data, remote)
	guard.release()
	require.Equal(t, int(fastWritePageSize), written)
	require.Equal(t, syscall.ENOSPC, errno)

	file := fastWrite.get(41)
	require.NotNil(t, file)
	file.RLock()
	require.True(t, file.checkpointPending)
	require.True(t, file.sealPending)
	require.True(t, file.metadataDirty)
	generation := activeGeneration(file)
	require.NotNil(t, generation)
	require.True(t, bitmapGet(generation.Chunks[0].Bitmap, 0))
	require.False(t, bitmapGet(generation.Chunks[0].Bitmap, 1))
	file.RUnlock()

	fastWrite.writeFileHook = nil
	require.Zero(t, fastWrite.Checkpoint(41))
	require.NoError(t, fastWrite.close())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	got := make([]byte, fastWritePageSize)
	n, errno := fastWrite.Read(41, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, data[:fastWritePageSize], got)
}

func TestFastWritePartialZeroRangeFailureKeepsCompletedPageDirty(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	remote := func(_ uint64, dst []byte) (int, syscall.Errno) {
		for index := range dst {
			dst[index] = 'r'
		}
		return len(dst), 0
	}
	var writes atomic.Int32
	fastWrite.writeFileHook = func(file *os.File, data []byte, off int64) (int, error) {
		if writes.Add(1) == 2 {
			return 0, syscall.EIO
		}
		return writeFullAtCount(file, data, off)
	}
	// 整页清零只置位图（bit=1 + hole = 零页），没有可失败的写 I/O，因此用跨
	// chunk 边界的部分页清零构造"第 2 次数据写失败"：页1（chunk0 尾页后半）
	// RMW 成功提交，页2（chunk1 首页前半）RMW 失败，不提交。
	delta := fastWritePageSize / 8
	start := uint64(meta.ChunkSize) - fastWritePageSize + delta
	require.Equal(t, syscall.EIO, fastWrite.ZeroRange(
		42, uint64(meta.ChunkSize)+fastWritePageSize, start, fastWritePageSize*2-delta*2, true, remote))

	file := fastWrite.get(42)
	require.NotNil(t, file)
	file.RLock()
	require.True(t, file.checkpointPending)
	require.True(t, file.sealPending)
	require.True(t, file.metadataDirty)
	generation := activeGeneration(file)
	require.True(t, bitmapGet(generation.Chunks[0].Bitmap, fastWritePagesPerChunk-1))
	require.False(t, bitmapGet(generation.Chunks[1].Bitmap, 0))
	file.RUnlock()

	fastWrite.writeFileHook = nil
	require.Zero(t, fastWrite.Checkpoint(42))
	require.NoError(t, fastWrite.close())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	got := bytes.Repeat([]byte{'x'}, int(fastWritePageSize))
	n, errno := fastWrite.Read(42, start, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	// 已完成的页1尾部为零，失败的页2头部回落到远端数据 'r'
	want := append(bytes.Repeat([]byte{0}, int(fastWritePageSize-delta)),
		bytes.Repeat([]byte{'r'}, int(delta))...)
	require.Equal(t, want, got)
}

func TestFastWriteFDEvictionFailureRollsBackNewEntry(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	fastWrite.fdLimit = 1
	remote := fastWriteTestRemote(nil)
	page := bytes.Repeat([]byte{'f'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(43, meta.ChunkSize*2, 0, page, remote))

	expected := errors.New("fdatasync failed")
	fastWrite.syncFileHook = func(*os.File) error { return expected }
	require.Equal(t, syscall.EIO, fastWrite.Write(43, meta.ChunkSize*2, meta.ChunkSize, page, remote))
	firstKey := fastWriteFDKey{inode: 43, gen: 1, chunk: 0}
	secondKey := fastWriteFDKey{inode: 43, gen: 1, chunk: 1}
	require.Contains(t, fastWrite.fds, firstKey)
	require.NotContains(t, fastWrite.fds, secondKey)
	require.Len(t, fastWrite.fds, 1)

	fastWrite.syncFileHook = nil
	require.Zero(t, fastWrite.Write(43, meta.ChunkSize*2, meta.ChunkSize, page, remote))
	require.Contains(t, fastWrite.fds, secondKey)
}

func TestFastWriteMetadataFlushUsesMutationEpoch(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	file, err := fastWrite.getOrCreate(45, 0)
	require.NoError(t, err)

	file.Lock()
	markFastWriteMutationLocked(file)
	file.Unlock()
	firstEpoch, dirty, ok := fastWrite.metadataState(45)
	require.True(t, ok)
	require.True(t, dirty)

	file.Lock()
	markFastWriteMutationLocked(file)
	file.Unlock()
	fastWrite.metadataFlushed(45, firstEpoch)
	secondEpoch, dirty, ok := fastWrite.metadataState(45)
	require.True(t, ok)
	require.True(t, dirty)
	require.Greater(t, secondEpoch, firstEpoch)

	fastWrite.metadataFlushed(45, secondEpoch)
	_, dirty, _ = fastWrite.metadataState(45)
	require.False(t, dirty)
}

func TestFastWriteSealUsesCloneBeforeRetiringActive(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	content := bytes.Repeat([]byte{'c'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(46, fastWritePageSize, 0, content, fastWriteTestRemote(nil)))

	var cloneCalls atomic.Int32
	fastWrite.cloneRangeHook = func(dstFD, srcFD int, srcOff, dstOff, length uint64) error {
		if srcOff != 0 || dstOff != 0 || length != fastWriteChunkFileSize {
			return fmt.Errorf("unexpected clone range %d:%d+%d", srcOff, dstOff, length)
		}
		if _, statErr := os.Stat(fastWrite.generationDir(46, fastWriteActive, 1)); statErr != nil {
			return fmt.Errorf("active generation disappeared before clone: %w", statErr)
		}
		if _, statErr := os.Stat(fastWrite.generationDir(46, fastWriteSealed, 1)); !os.IsNotExist(statErr) {
			return fmt.Errorf("sealed generation was published before clone")
		}
		if truncateErr := syscall.Ftruncate(dstFD, int64(length)); truncateErr != nil {
			return truncateErr
		}
		srcCopy, duplicateErr := syscall.Dup(srcFD)
		if duplicateErr != nil {
			return duplicateErr
		}
		source := os.NewFile(uintptr(srcCopy), "fast-write-clone-source")
		defer source.Close()
		dstCopy, duplicateErr := syscall.Dup(dstFD)
		if duplicateErr != nil {
			return duplicateErr
		}
		target := os.NewFile(uintptr(dstCopy), "fast-write-clone-target")
		defer target.Close()
		buffer := make([]byte, fastWritePageSize)
		for _, offset := range []int64{0, int64(meta.ChunkSize)} {
			if readErr := readExactAt(source, buffer, offset); readErr != nil {
				return readErr
			}
			if writeErr := writeFullAt(target, buffer, offset); writeErr != nil {
				return writeErr
			}
		}
		cloneCalls.Add(1)
		return nil
	}

	sealed, errno := fastWrite.Seal(46)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), sealed)
	require.Equal(t, int32(1), cloneCalls.Load())
	_, err = os.Stat(fastWrite.generationDir(46, fastWriteActive, 1))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(fastWrite.generationDir(46, fastWriteSealed, 1))
	require.NoError(t, err)
	_, err = os.Stat(fastWrite.generationDir(46, fastWriteActive, 2))
	require.NoError(t, err)
	got := make([]byte, len(content))
	n, errno := fastWrite.Read(46, 0, got, fastWriteTestRemote(nil))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, content, got)
}

func TestFastWriteSealFallsBackToSparseBitmapCopy(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	first := bytes.Repeat([]byte{'d'}, int(fastWritePageSize))
	explicitZero := make([]byte, fastWritePageSize)
	content := append(append([]byte(nil), first...), explicitZero...)
	require.Zero(t, fastWrite.Write(47, uint64(len(content)), 0, content, fastWriteTestRemote(nil)))

	var cloneCalls atomic.Int32
	fastWrite.cloneRangeHook = func(_, _ int, _, _, _ uint64) error {
		cloneCalls.Add(1)
		return syscall.EOPNOTSUPP
	}
	sealed, errno := fastWrite.Seal(47)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), sealed)
	require.Equal(t, int32(1), cloneCalls.Load())

	path := filepath.Join(fastWrite.generationDir(47, fastWriteSealed, sealed), chunkFileName(0))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(fastWriteChunkFileSize), info.Size())
	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.Less(t, stat.Blocks*512, int64(fastWriteChunkFileSize/2),
		"bitmap fallback should not materialize the 64 MiB sparse chunk")
	chunk, length, _, err := readChunkFooter(path, sealed, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(len(content)), length)
	require.True(t, bitmapGet(chunk.Bitmap, 0))
	require.True(t, bitmapGet(chunk.Bitmap, 1), "an explicit zero page must remain locally authoritative")

	got := make([]byte, len(content))
	n, errno := fastWrite.Read(47, 0, got, fastWriteTestRemote(bytes.Repeat([]byte{'r'}, len(content))))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, content, got)
}

func TestFastWriteSealCloneFailureLeavesActiveGeneration(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	content := bytes.Repeat([]byte{'e'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(48, fastWritePageSize, 0, content, fastWriteTestRemote(nil)))
	require.Zero(t, fastWrite.Checkpoint(48))

	fastWrite.cloneRangeHook = func(_, _ int, _, _, _ uint64) error {
		return syscall.EOPNOTSUPP
	}
	fastWrite.syncFileHook = func(*os.File) error {
		return syscall.EIO
	}
	_, errno := fastWrite.Seal(48)
	require.Equal(t, syscall.EIO, errno)
	_, err = os.Stat(fastWrite.generationDir(48, fastWriteActive, 1))
	require.NoError(t, err)
	_, err = os.Stat(fastWrite.generationDir(48, fastWriteSealed, 1))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(fastWrite.transientGenerationDir(48, fastWriteSealing, 1))
	require.ErrorIs(t, err, os.ErrNotExist)

	fastWrite.cloneRangeHook = func(_, _ int, _, _, _ uint64) error {
		return syscall.EOPNOTSUPP
	}
	fastWrite.syncFileHook = nil
	sealed, errno := fastWrite.Seal(48)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), sealed)
}

func TestFastWriteSealRetryCleansIncompleteClone(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	require.Zero(t, fastWrite.Write(
		49, fastWritePageSize, 0, bytes.Repeat([]byte{'f'}, int(fastWritePageSize)), fastWriteTestRemote(nil)))
	require.Zero(t, fastWrite.Checkpoint(49))
	sealing := fastWrite.transientGenerationDir(49, fastWriteSealing, 1)
	fastWrite.cloneRangeHook = func(_, _ int, _, _, _ uint64) error {
		return syscall.EOPNOTSUPP
	}
	fastWrite.syncFileHook = func(*os.File) error {
		return syscall.EIO
	}
	fastWrite.removeAllHook = func(path string) error {
		if path == sealing {
			return syscall.EBUSY
		}
		return os.RemoveAll(path)
	}

	_, errno := fastWrite.Seal(49)
	require.Equal(t, syscall.EIO, errno)
	_, err = os.Stat(sealing)
	require.NoError(t, err)

	fastWrite.syncFileHook = nil
	fastWrite.removeAllHook = nil
	sealed, errno := fastWrite.Seal(49)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), sealed)
	_, err = os.Stat(sealing)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestFastWriteSealRollsBackWhenNextGenerationCannotBeCreated(t *testing.T) {
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	require.Zero(t, fastWrite.Write(44, 0, 0, []byte("seal"), fastWriteTestRemote(nil)))

	conflict := fastWrite.generationDir(44, fastWriteActive, 2)
	require.NoError(t, os.WriteFile(conflict, []byte("conflict"), 0600))
	_, errno := fastWrite.Seal(44)
	require.Equal(t, syscall.EIO, errno)
	_, err = os.Stat(fastWrite.generationDir(44, fastWriteActive, 1))
	require.NoError(t, err)
	_, err = os.Stat(fastWrite.generationDir(44, fastWriteSealed, 1))
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, os.Remove(conflict))
	generation, errno := fastWrite.Seal(44)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), generation)
}

func TestFastWriteBitmapAndUploadRunBoundaries(t *testing.T) {
	bitmap := make([]byte, fastWriteBitmapSize)
	for _, page := range []uint32{0, 1, 3, fastWritePagesPerChunk - 1} {
		bitmapSet(bitmap, page)
	}
	require.Equal(t, [][2]uint32{
		{0, 2},
		{3, 4},
		{fastWritePagesPerChunk - 1, fastWritePagesPerChunk},
	}, bitmapRuns(bitmap))

	first := make([]byte, fastWriteBitmapSize)
	bitmapSet(first, fastWritePagesPerChunk-1)
	second := make([]byte, fastWriteBitmapSize)
	bitmapSet(second, 0)
	bitmapSet(second, 1)
	generation := &fastWriteGeneration{
		ID:     1,
		Length: uint64(meta.ChunkSize) + fastWritePageSize + 123,
		Chunks: map[uint32]*fastWriteChunk{
			0: {Index: 0, Bitmap: first},
			1: {Index: 1, Bitmap: second},
		},
	}
	require.Equal(t, []fastWriteUploadRun{
		{Chunk: 0, StartPage: fastWritePagesPerChunk - 1, EndPage: fastWritePagesPerChunk},
		{Chunk: 1, StartPage: 0, EndPage: 2},
	}, buildUploadRuns(generation))
}

func TestFastWriteRandomizedBoundaryModel(t *testing.T) {
	const operations = 120
	remoteLength := uint64(meta.ChunkSize) + fastWritePageSize*3
	remoteByte := func(off uint64) byte {
		return byte((off*131 + 17) % 251)
	}
	remote := func(off uint64, dst []byte) (int, syscall.Errno) {
		for index := range dst {
			dst[index] = remoteByte(off + uint64(index))
		}
		return len(dst), 0
	}
	anchors := []uint64{
		0,
		1,
		fastWritePageSize - 1,
		fastWritePageSize,
		fastWritePageSize*2 - 17,
		uint64(meta.ChunkSize) - fastWritePageSize,
		uint64(meta.ChunkSize) - 1,
		uint64(meta.ChunkSize),
		uint64(meta.ChunkSize) + fastWritePageSize - 1,
	}

	for _, seed := range []int64{1, 17, 2026} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			root := t.TempDir()
			fastWrite, err := openFastWrite(root)
			require.NoError(t, err)
			defer func() { require.NoError(t, fastWrite.close()) }()
			rng := rand.New(rand.NewSource(seed))
			model := make(map[uint64][]byte)

			modelPage := func(page uint64) []byte {
				if data := model[page]; data != nil {
					return data
				}
				data := make([]byte, fastWritePageSize)
				base := page * fastWritePageSize
				for index := range data {
					data[index] = remoteByte(base + uint64(index))
				}
				model[page] = data
				return data
			}
			apply := func(off uint64, data []byte) {
				for len(data) > 0 {
					page := off / fastWritePageSize
					pageOffset := int(off % fastWritePageSize)
					n := min(len(data), int(fastWritePageSize)-pageOffset)
					copy(modelPage(page)[pageOffset:pageOffset+n], data[:n])
					off += uint64(n)
					data = data[n:]
				}
			}
			expected := func(off uint64, length int) []byte {
				data := make([]byte, length)
				for index := range data {
					absolute := off + uint64(index)
					if page := model[absolute/fastWritePageSize]; page != nil {
						data[index] = page[absolute%fastWritePageSize]
					} else {
						data[index] = remoteByte(absolute)
					}
				}
				return data
			}
			check := func(off uint64, length int) {
				if off+uint64(length) > remoteLength {
					length = int(remoteLength - off)
				}
				got := make([]byte, length)
				n, errno := fastWrite.Read(50, off, got, remote)
				require.Zero(t, errno)
				require.Equal(t, len(got), n)
				require.Equal(t, expected(off, length), got)
			}

			for operation := 0; operation < operations; operation++ {
				off := anchors[rng.Intn(len(anchors))]
				if room := remoteLength - off; room > 1 {
					off += uint64(rng.Intn(int(min(room, 97))))
				}
				length := 1 + rng.Intn(int(fastWritePageSize*2))
				if off+uint64(length) > remoteLength {
					length = int(remoteLength - off)
				}
				if rng.Intn(4) == 0 {
					require.Zero(t, fastWrite.ZeroRange(50, remoteLength, off, uint64(length), true, remote))
					apply(off, make([]byte, length))
				} else {
					data := make([]byte, length)
					_, err = rng.Read(data)
					require.NoError(t, err)
					require.Zero(t, fastWrite.Write(50, remoteLength, off, data, remote))
					apply(off, data)
				}
				start := off
				if start > 31 {
					start -= 31
				}
				check(start, int(fastWritePageSize*3))

				if operation > 0 && operation%29 == 0 {
					require.Zero(t, fastWrite.Checkpoint(50))
				}
				if operation > 0 && operation%37 == 0 {
					_, errno := fastWrite.Seal(50)
					require.Zero(t, errno)
				}
				if operation > 0 && operation%53 == 0 {
					require.Zero(t, fastWrite.Checkpoint(50))
					require.NoError(t, fastWrite.close())
					fastWrite, err = openFastWrite(root)
					require.NoError(t, err)
				}
			}
			require.Zero(t, fastWrite.Checkpoint(50))
			require.NoError(t, fastWrite.close())
			fastWrite, err = openFastWrite(root)
			require.NoError(t, err)
			for _, off := range anchors {
				check(off, int(fastWritePageSize*2))
			}
		})
	}
}

func TestFastWriteCheckpointRetriesFileSyncFailures(t *testing.T) {
	for _, failAt := range []int32{1, 2} {
		t.Run(fmt.Sprintf("sync-%d", failAt), func(t *testing.T) {
			root := t.TempDir()
			fastWrite, err := openFastWrite(root)
			require.NoError(t, err)
			page := bytes.Repeat([]byte{byte('0' + failAt)}, int(fastWritePageSize))
			require.Zero(t, fastWrite.Write(51, 0, 0, page, fastWriteTestRemote(nil)))

			var calls atomic.Int32
			fastWrite.syncFileHook = func(*os.File) error {
				if calls.Add(1) == failAt {
					return errors.New("injected sync failure")
				}
				return nil
			}
			require.Equal(t, syscall.EIO, fastWrite.Checkpoint(51))
			file := fastWrite.get(51)
			require.NotNil(t, file)
			file.RLock()
			require.True(t, file.checkpointPending)
			file.RUnlock()
			require.NotEmpty(t, fastWrite.dirtyChunks)

			fastWrite.syncFileHook = nil
			require.Zero(t, fastWrite.Checkpoint(51))
			require.Empty(t, fastWrite.dirtyChunks)
			require.NoError(t, fastWrite.close())

			fastWrite, err = openFastWrite(root)
			require.NoError(t, err)
			defer fastWrite.close()
			got := make([]byte, fastWritePageSize)
			n, errno := fastWrite.Read(51, 0, got, fastWriteTestRemote(nil))
			require.Zero(t, errno)
			require.Equal(t, len(got), n)
			require.Equal(t, page, got)
		})
	}
}

func TestFastWriteSealRetriesDirectorySync(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	require.Zero(t, fastWrite.Write(52, 0, 0, []byte("seal-sync"), fastWriteTestRemote(nil)))

	var inodeDirSyncs atomic.Int32
	fastWrite.syncDirHook = func(path string) error {
		if path == fastWrite.inodeDir(52) {
			if inodeDirSyncs.Add(1) == 2 {
				return errors.New("injected inode directory sync failure")
			}
		}
		return nil
	}
	_, errno := fastWrite.Seal(52)
	require.Equal(t, syscall.EIO, errno)
	sealed, errno := fastWrite.Seal(52)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), sealed)
	require.GreaterOrEqual(t, inodeDirSyncs.Load(), int32(3),
		"a successful retry must confirm the already-created generation boundary")
}

func TestFastWriteConcurrentWriteCheckpointAndSeal(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	const (
		writers        = 8
		pagesPerWriter = 8
		rounds         = 8
	)
	length := uint64(writers*pagesPerWriter) * fastWritePageSize
	remote := fastWriteTestRemote(make([]byte, length))
	start := make(chan struct{})
	errs := make(chan error, writers+1)
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			<-start
			page := bytes.Repeat([]byte{byte(writer + 1)}, int(fastWritePageSize))
			for round := 0; round < rounds; round++ {
				for index := 0; index < pagesPerWriter; index++ {
					off := uint64(writer*pagesPerWriter+index) * fastWritePageSize
					if errno := fastWrite.Write(53, length, off, page, remote); errno != 0 {
						errs <- fmt.Errorf("writer %d: %s", writer, errno)
						return
					}
				}
			}
		}(writer)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for iteration := 0; iteration < rounds; iteration++ {
			var errno syscall.Errno
			if iteration%2 == 0 {
				errno = fastWrite.Checkpoint(53)
			} else {
				_, errno = fastWrite.Seal(53)
			}
			if errno != 0 {
				errs <- fmt.Errorf("maintenance iteration %d: %s", iteration, errno)
				return
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err = range errs {
		require.NoError(t, err)
	}
	require.Zero(t, fastWrite.Checkpoint(53))

	got := make([]byte, length)
	n, errno := fastWrite.Read(53, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	for writer := 0; writer < writers; writer++ {
		begin := writer * pagesPerWriter * int(fastWritePageSize)
		end := begin + pagesPerWriter*int(fastWritePageSize)
		require.Equal(t, bytes.Repeat([]byte{byte(writer + 1)}, end-begin), got[begin:end])
	}
}

func TestVFSFastWriteCollectsAndUploadsSelectedPaths(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(120, 0, []uint32{0}))

	dir, errno := v.Mkdir(ctx, meta.RootInode, "fast-write-tree", 0755, 0)
	require.Zero(t, errno)
	first, firstFH, errno := v.Create(ctx, dir.Inode, "first", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	second, secondFH, errno := v.Create(ctx, dir.Inode, "second", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	clean, _, errno := v.Create(ctx, dir.Inode, "clean", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	root, rootFH, errno := v.Create(ctx, meta.RootInode, "fast-write-root", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, first.Inode, []byte("first"), 0, firstFH))
	require.Zero(t, v.Write(ctx, second.Inode, []byte("second"), 0, secondFH))
	require.Zero(t, v.Write(ctx, root.Inode, []byte("root"), 0, rootFH))

	inodes, errno := v.collectFastWriteInodes(ctx, []string{
		"fast-write-tree",
		fmt.Sprintf("inode:%d", first.Inode),
		"fast-write-root",
	})
	require.Zero(t, errno)
	require.Len(t, inodes, 4)
	expected := map[Ino]struct{}{
		first.Inode: {}, second.Inode: {}, clean.Inode: {}, root.Inode: {},
	}
	for index, inode := range inodes {
		_, ok := expected[inode]
		require.True(t, ok)
		if index > 0 {
			require.Less(t, inodes[index-1], inode)
		}
	}

	stat, errno := v.uploadFastWritePaths(ctx, []string{"fast-write-tree"}, false)
	require.Zero(t, errno)
	require.Equal(t, "completed", stat.State)
	require.Equal(t, uint64(2), stat.FileCount)
	require.Equal(t, []Ino{root.Inode}, fastWrite.listInodes())

	stat, errno = v.uploadFastWritePaths(ctx, nil, false)
	require.Zero(t, errno)
	require.Equal(t, uint64(1), stat.FileCount)
	require.Empty(t, fastWrite.listInodes())
}

func TestVFSFastWriteFlushAllCheckpointsRecoverableState(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(121, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-flush-all", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	content := bytes.Repeat([]byte{'f'}, int(fastWritePageSize*2))
	require.Zero(t, v.Write(ctx, entry.Inode, content, 0, fh))
	require.NoError(t, v.FlushAll(""))
	require.NoError(t, fastWrite.close())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	got := make([]byte, len(content))
	n, errno := fastWrite.Read(entry.Inode, 0, got, fastWriteTestRemote(nil))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, content, got)
}

func TestVFSFastWriteWritebackStagingFailureIsRetryable(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	v.Conf.Chunk.Writeback = true
	store := &recordingFastWriteStagingStore{ChunkStore: v.Store, failAt: 2}
	v.Store = store
	ctx := NewLogContext(meta.NewContext(122, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-staging-retry", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	first := bytes.Repeat([]byte{'a'}, int(fastWritePageSize))
	third := bytes.Repeat([]byte{'c'}, int(fastWritePageSize))
	require.Zero(t, v.Write(ctx, entry.Inode, first, 0, fh))
	require.Zero(t, v.Write(ctx, entry.Inode, third, fastWritePageSize*2, fh))

	require.Equal(t, syscall.EIO, v.uploadFastWriteInode(ctx, entry.Inode, &UploadFastWriteResponse{}))
	var slices []meta.Slice
	require.Zero(t, v.Meta.Read(ctx, entry.Inode, 0, &slices))
	for _, slice := range slices {
		require.Zero(t, slice.Id, "metadata became visible after a partial staging flush")
	}
	require.NotEmpty(t, fastWrite.listInodes())

	store.failAt = 0
	require.Zero(t, v.uploadFastWriteInode(ctx, entry.Inode, &UploadFastWriteResponse{}))
	require.GreaterOrEqual(t, store.calls.Load(), int32(4))
	require.Empty(t, fastWrite.listInodes())
	got := make([]byte, fastWritePageSize*3)
	n, errno := v.Read(ctx, entry.Inode, got, 0, fh)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, first, got[:fastWritePageSize])
	require.True(t, allZero(got[fastWritePageSize:fastWritePageSize*2]))
	require.Equal(t, third, got[fastWritePageSize*2:])
}

func TestVFSFastWriteRenameHardlinkAndLastCloseLifecycle(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	v.Conf.FuseOpts.EnableWriteback = true
	ctx := NewLogContext(meta.NewContext(123, 0, []uint32{0}))

	source, sourceFH, errno := v.Create(ctx, meta.RootInode, "fast-write-source", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	replaced, replacedFH, errno := v.Create(ctx, meta.RootInode, "fast-write-replaced", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, source.Inode, []byte("source"), 0, sourceFH))
	require.Zero(t, v.Write(ctx, replaced.Inode, []byte("replaced"), 0, replacedFH))

	require.Zero(t, v.Rename(ctx, meta.RootInode, "fast-write-source",
		meta.RootInode, "fast-write-replaced", 0))
	require.True(t, fastWrite.isDeleted(replaced.Inode))
	v.Release(ctx, replaced.Inode, replacedFH)
	require.Eventually(t, func() bool {
		return fastWrite.get(replaced.Inode) == nil
	}, 5*time.Second, 10*time.Millisecond)

	_, errno = v.Link(ctx, source.Inode, meta.RootInode, "fast-write-linked")
	require.Zero(t, errno)
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "fast-write-replaced"))
	require.False(t, fastWrite.isDeleted(source.Inode))
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "fast-write-linked"))
	require.True(t, fastWrite.isDeleted(source.Inode))
	v.Release(ctx, source.Inode, sourceFH)
	require.Eventually(t, func() bool {
		return fastWrite.get(source.Inode) == nil
	}, 5*time.Second, 10*time.Millisecond)
}

func TestFastWriteCheckpointedGenerationIsImmutableAfterFailedOverwrite(t *testing.T) {
	root := t.TempDir()
	remote := fastWriteTestRemote(nil)
	oldPage := bytes.Repeat([]byte{'o'}, int(fastWritePageSize))
	newPage := bytes.Repeat([]byte{'n'}, int(fastWritePageSize))

	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.Zero(t, fastWrite.Write(60, fastWritePageSize, 0, oldPage, remote))
	require.Zero(t, fastWrite.Checkpoint(60))
	require.Zero(t, fastWrite.Write(60, fastWritePageSize, 0, newPage, remote))

	file := fastWrite.get(60)
	require.NotNil(t, file)
	file.RLock()
	require.Len(t, file.generations, 2)
	require.Equal(t, fastWriteSealed, file.generations[0].State)
	require.Equal(t, fastWriteActive, file.generations[1].State)
	file.RUnlock()

	fastWrite.syncFileHook = func(*os.File) error { return errors.New("injected overwrite sync failure") }
	require.Equal(t, syscall.EIO, fastWrite.Checkpoint(60))
	fastWrite.syncFileHook = nil
	require.NoError(t, fastWrite.abort())

	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	got := make([]byte, fastWritePageSize)
	n, errno := fastWrite.Read(60, 0, got, remote)
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, oldPage, got)
}

func TestVFSFastWriteCheckpointFlushesMetadataBeforeData(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	orderedMeta := &orderingFastWriteMeta{Meta: v.Meta}
	v.Meta = orderedMeta
	ctx := NewLogContext(meta.NewContext(124, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-checkpoint-order", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, bytes.Repeat([]byte{'m'}, int(fastWritePageSize)), 0, fh))
	fastWrite.syncFileHook = func(*os.File) error {
		if !orderedMeta.mtimeFlushed.Load() {
			return errors.New("data checkpoint ran before metadata flush")
		}
		return nil
	}
	require.Zero(t, v.Fsync(ctx, entry.Inode, 0, fh))
}

func TestVFSFastWriteSparseWriteFailureRestoresOriginalLength(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	ctx := NewLogContext(meta.NewContext(125, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-sparse-failure", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	var attr Attr
	require.Zero(t, v.Truncate(ctx, entry.Inode, int64(fastWritePageSize), fh, &attr))
	fastWrite.writeFileHook = func(*os.File, []byte, int64) (int, error) {
		return 0, syscall.ENOSPC
	}
	written, errno := v.WriteWithCount(ctx, entry.Inode, make([]byte, fastWritePageSize),
		fastWritePageSize*3, fh)
	require.Zero(t, written)
	require.Equal(t, syscall.ENOSPC, errno)
	require.Zero(t, v.Meta.GetAttr(ctx, entry.Inode, &attr))
	require.Equal(t, fastWritePageSize, attr.Length)
	length, ok := fastWrite.Length(entry.Inode)
	require.True(t, ok)
	require.Equal(t, fastWritePageSize, length)
}

func TestVFSFastWriteCopyRangeRestoresCommittedLength(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	ctx := NewLogContext(meta.NewContext(126, 0, []uint32{0}))

	source, sourceFH, errno := v.Create(ctx, meta.RootInode, "fast-write-copy-source", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	sourceData := bytes.Repeat([]byte{'s'}, int(fastWritePageSize*2))
	require.Zero(t, v.Write(ctx, source.Inode, sourceData, 0, sourceFH))
	require.Zero(t, v.uploadFastWriteInode(ctx, source.Inode, &UploadFastWriteResponse{}))

	t.Run("zero bytes", func(t *testing.T) {
		target, targetFH, createErrno := v.Create(ctx, meta.RootInode, "fast-write-copy-zero", 0644, 0, syscall.O_RDWR)
		require.Zero(t, createErrno)
		fastWrite.writeFileHook = func(*os.File, []byte, int64) (int, error) {
			return 0, syscall.ENOSPC
		}
		copied, copyErrno := v.CopyFileRange(ctx, source.Inode, sourceFH, 0,
			target.Inode, targetFH, fastWritePageSize*3, fastWritePageSize, 0)
		require.Zero(t, copied)
		require.Equal(t, syscall.ENOSPC, copyErrno)
		var attr Attr
		require.Zero(t, v.Meta.GetAttr(ctx, target.Inode, &attr))
		require.Zero(t, attr.Length)
	})

	t.Run("one page", func(t *testing.T) {
		target, targetFH, createErrno := v.Create(ctx, meta.RootInode, "fast-write-copy-partial", 0644, 0, syscall.O_RDWR)
		require.Zero(t, createErrno)
		fastWrite.writeFileHook = func(file *os.File, data []byte, off int64) (int, error) {
			written, writeErr := writeFullAtCount(file, data[:fastWritePageSize], off)
			if writeErr != nil {
				return written, writeErr
			}
			return written, syscall.ENOSPC
		}
		copied, copyErrno := v.CopyFileRange(ctx, source.Inode, sourceFH, 0,
			target.Inode, targetFH, fastWritePageSize*2, fastWritePageSize*2, 0)
		require.Equal(t, fastWritePageSize, copied)
		require.Equal(t, syscall.ENOSPC, copyErrno)
		var attr Attr
		require.Zero(t, v.Meta.GetAttr(ctx, target.Inode, &attr))
		require.Equal(t, fastWritePageSize*3, attr.Length)
	})
	fastWrite.writeFileHook = nil
}

func TestVFSFastWriteCopyRangeZeroLengthIsNoop(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	ctx := NewLogContext(meta.NewContext(133, 0, []uint32{0}))

	source, sourceFH, errno := v.Create(ctx, meta.RootInode, "fast-write-zero-copy-source", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, source.Inode, make([]byte, fastWritePageSize), 0, sourceFH))
	target, targetFH, errno := v.Create(ctx, meta.RootInode, "fast-write-zero-copy-target", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)

	copied, errno := v.CopyFileRange(ctx, source.Inode, sourceFH, 0,
		target.Inode, targetFH, fastWritePageSize*4, 0, 0)
	require.Zero(t, copied)
	require.Zero(t, errno)
	var attr Attr
	require.Zero(t, v.Meta.GetAttr(ctx, target.Inode, &attr))
	require.Zero(t, attr.Length)

	copied, errno = v.CopyFileRange(ctx, source.Inode, sourceFH, 0,
		source.Inode, sourceFH, fastWritePageSize*4, 0, 0)
	require.Zero(t, copied)
	require.Zero(t, errno)
	require.Zero(t, v.Meta.GetAttr(ctx, source.Inode, &attr))
	require.Equal(t, fastWritePageSize, attr.Length)
}

func TestVFSFastWriteLinkClearsDeletedMarkerBeforeMetadata(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	ctx := NewLogContext(meta.NewContext(127, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-link-source", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, []byte("linked"), 0, fh))
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "fast-write-link-source"))
	require.True(t, fastWrite.isDeleted(entry.Inode))

	marker := filepath.Join(fastWrite.inodeDir(entry.Inode), ".deleted")
	checkingMeta := &checkingFastWriteLinkMeta{Meta: v.Meta}
	checkingMeta.beforeLink = func() {
		require.False(t, fastWrite.isDeleted(entry.Inode))
		_, statErr := os.Stat(marker)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
	v.Meta = checkingMeta
	_, errno = v.Link(ctx, entry.Inode, meta.RootInode, "fast-write-link-restored")
	require.Equal(t, syscall.ENOENT, errno)
	require.True(t, fastWrite.isDeleted(entry.Inode))
	_, err = os.Stat(marker)
	require.NoError(t, err)
}

func TestFastWriteRecoveryReconcilesRelinkedDeletedMarker(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = root
	ctx := NewLogContext(meta.NewContext(128, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-relink-crash", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	content := bytes.Repeat([]byte{'r'}, int(fastWritePageSize))
	require.Zero(t, v.Write(ctx, entry.Inode, content, 0, fh))
	require.Zero(t, v.Fsync(ctx, entry.Inode, 0, fh))
	require.NoError(t, fastWrite.markDeleted(entry.Inode))
	require.True(t, fastWrite.isDeleted(entry.Inode))
	require.NoError(t, fastWrite.abort())

	recovered, err := openFastWrite(root)
	require.NoError(t, err)
	defer recovered.close()
	require.NoError(t, recovered.reconcileMetadata(v.Meta, nil))
	require.False(t, recovered.isDeleted(entry.Inode))
	got := make([]byte, len(content))
	n, errno := recovered.Read(entry.Inode, 0, got, fastWriteTestRemote(nil))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, content, got)
}

func TestFastWriteFinishUploadDetachesBeforeBestEffortCleanup(t *testing.T) {
	root := t.TempDir()
	content := bytes.Repeat([]byte{'u'}, int(fastWritePageSize))
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	require.Zero(t, fastWrite.Write(61, fastWritePageSize, 0, content, fastWriteTestRemote(nil)))
	generation, errno := fastWrite.Seal(61)
	require.Zero(t, errno)
	require.NotZero(t, generation)

	fastWrite.removeAllHook = func(path string) error {
		if path == fastWrite.garbageGenerationDir(61, generation) {
			return errors.New("injected cleanup failure")
		}
		return os.RemoveAll(path)
	}
	require.Zero(t, fastWrite.finishUpload(61, generation))
	file := fastWrite.get(61)
	require.NotNil(t, file)
	file.RLock()
	require.Len(t, file.generations, 1)
	require.Equal(t, fastWriteActive, file.generations[0].State)
	file.RUnlock()
	got := make([]byte, len(content))
	n, errno := fastWrite.Read(61, 0, got, fastWriteTestRemote(content))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, content, got)
	_, err = os.Stat(fastWrite.garbageGenerationDir(61, generation))
	require.NoError(t, err)

	fastWrite.removeAllHook = nil
	require.NoError(t, fastWrite.abort())
	fastWrite, err = openFastWrite(root)
	require.NoError(t, err)
	defer fastWrite.close()
	require.Nil(t, fastWrite.get(61))
	_, err = os.Stat(filepath.Join(root, "inodes", "61"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestFastWriteWriteUsesLockedLengthBaseline(t *testing.T) {
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	page := bytes.Repeat([]byte{'l'}, int(fastWritePageSize))
	require.Zero(t, fastWrite.Write(62, 0, 0, page, fastWriteTestRemote(nil)))

	guard, errno := fastWrite.beginWrite(62, 0, fastWritePageSize*2, fastWritePageSize)
	require.Zero(t, errno)
	require.Equal(t, fastWritePageSize, guard.originalLength)
	guard.release()
}

func TestVFSFastWriteCopyRangeSerializesConcurrentWriter(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	copyCtx := NewLogContext(meta.NewContext(129, 0, []uint32{0}))
	writeCtx := NewLogContext(meta.NewContext(130, 0, []uint32{0}))

	source, sourceFH, errno := v.Create(copyCtx, meta.RootInode, "fast-write-concurrent-copy-source",
		0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	page := bytes.Repeat([]byte{'c'}, int(fastWritePageSize))
	require.Zero(t, v.Write(copyCtx, source.Inode, page, 0, sourceFH))
	require.Zero(t, v.uploadFastWriteInode(copyCtx, source.Inode, &UploadFastWriteResponse{}))

	target, targetFH, errno := v.Create(copyCtx, meta.RootInode, "fast-write-concurrent-copy-target",
		0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	_, secondFH, errno := v.Open(writeCtx, target.Inode, syscall.O_RDWR)
	require.Zero(t, errno)

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fastWrite.writeFileHook = func(file *os.File, data []byte, off int64) (int, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return 0, syscall.ENOSPC
		}
		return writeFullAtCount(file, data, off)
	}
	type copyResult struct {
		copied uint64
		errno  syscall.Errno
	}
	copyDone := make(chan copyResult, 1)
	go func() {
		copied, copyErrno := v.CopyFileRange(copyCtx, source.Inode, sourceFH, 0,
			target.Inode, targetFH, 0, fastWritePageSize, 0)
		copyDone <- copyResult{copied: copied, errno: copyErrno}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("copy did not reach the injected write")
	}

	type writeResult struct {
		written uint32
		errno   syscall.Errno
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		written, writeErrno := v.WriteWithCount(writeCtx, target.Inode, page, fastWritePageSize*4, secondFH)
		writeDone <- writeResult{written: written, errno: writeErrno}
	}()
	select {
	case result := <-writeDone:
		t.Fatalf("concurrent writer bypassed copy mutation guard: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	copyOutcome := <-copyDone
	require.Zero(t, copyOutcome.copied)
	require.Equal(t, syscall.ENOSPC, copyOutcome.errno)
	writeOutcome := <-writeDone
	require.Equal(t, uint32(fastWritePageSize), writeOutcome.written)
	require.Zero(t, writeOutcome.errno)
	fastWrite.writeFileHook = nil

	var attr Attr
	require.Zero(t, v.Meta.GetAttr(copyCtx, target.Inode, &attr))
	require.Equal(t, fastWritePageSize*5, attr.Length)
}

func TestFastWriteReconcileKeepsUnlinkedRecoveredHandle(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	require.NoError(t, v.Meta.NewSession(true))
	defer v.Meta.CloseSession()
	root := t.TempDir()
	fastWrite, err := openFastWrite(root)
	require.NoError(t, err)
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = root
	ctx := NewLogContext(meta.NewContext(131, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-unlinked-recovery", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	content := bytes.Repeat([]byte{'k'}, int(fastWritePageSize))
	require.Zero(t, v.Write(ctx, entry.Inode, content, 0, fh))
	require.Zero(t, v.Fsync(ctx, entry.Inode, 0, fh))
	require.Zero(t, v.Unlink(ctx, meta.RootInode, "fast-write-unlinked-recovery"))
	require.True(t, fastWrite.isDeleted(entry.Inode))
	tail := bytes.Repeat([]byte{'q'}, int(fastWritePageSize))
	require.Zero(t, v.Write(ctx, entry.Inode, tail, fastWritePageSize, fh))
	content = append(content, tail...)
	require.Zero(t, v.Fsync(ctx, entry.Inode, 0, fh))
	attrEntry, errno := v.GetAttr(ctx, entry.Inode, 1)
	require.Zero(t, errno)
	require.Equal(t, uint64(len(content)), attrEntry.Attr.Length)
	marker := filepath.Join(fastWrite.inodeDir(entry.Inode), ".deleted")
	require.NoError(t, os.Remove(marker))
	require.NoError(t, syncDir(fastWrite.inodeDir(entry.Inode)))
	require.NoError(t, fastWrite.abort())

	recovered, err := openFastWrite(root)
	require.NoError(t, err)
	defer recovered.close()
	require.NoError(t, recovered.reconcileMetadata(v.Meta, func(inode Ino) bool {
		return inode == entry.Inode
	}))
	require.True(t, recovered.isDeleted(entry.Inode))
	_, err = os.Stat(marker)
	require.NoError(t, err)
	require.NotContains(t, recovered.listInodes(), entry.Inode)
	require.Contains(t, recovered.checkpointInodes(), entry.Inode)
	got := make([]byte, len(content))
	n, errno := recovered.Read(entry.Inode, 0, got, fastWriteTestRemote(nil))
	require.Zero(t, errno)
	require.Equal(t, len(got), n)
	require.Equal(t, content, got)
}

func TestVFSFastWriteDumpUsesOverlayLength(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	fastWrite, err := openFastWrite(t.TempDir())
	require.NoError(t, err)
	defer fastWrite.close()
	v.fastWrite = fastWrite
	v.Conf.FastWriteDir = fastWrite.root
	ctx := NewLogContext(meta.NewContext(132, 0, []uint32{0}))

	entry, fh, errno := v.Create(ctx, meta.RootInode, "fast-write-dump-length", 0644, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	require.Zero(t, v.Write(ctx, entry.Inode, make([]byte, fastWritePageSize), fastWritePageSize*2, fh))
	statePath := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, v.dumpAllHandles(statePath))
	data, err := os.ReadFile(statePath)
	require.NoError(t, err)
	var saved state
	require.NoError(t, json.Unmarshal(data, &saved))
	require.Equal(t, fastWritePageSize*3, saved.Handler[fh].Length)
}

func TestVFSFastWriteKeepsHandleStateOnStartupFailure(t *testing.T) {
	base, _ := createTestVFS(nil, "")
	conf := *base.Conf
	conf.FastWriteDir = t.TempDir()
	statePath := filepath.Join(t.TempDir(), "invalid-state.json")
	require.NoError(t, os.WriteFile(statePath, []byte("{invalid"), 0600))
	t.Setenv("_FUSE_STATE_PATH", statePath)

	restarted := NewVFS(&conf, base.Meta, base.Store, nil, nil)
	require.Error(t, restarted.FastWriteInitError())
	_, err := os.Stat(statePath)
	require.NoError(t, err)
	_, err = os.Stat(statePath + ".bak")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func BenchmarkFastWriteRandomOverwrite(b *testing.B) {
	fastWrite, err := openFastWrite(b.TempDir())
	require.NoError(b, err)
	defer fastWrite.close()
	remote := fastWriteTestRemote(make([]byte, meta.ChunkSize))
	page := bytes.Repeat([]byte{'b'}, int(fastWritePageSize))
	b.SetBytes(int64(len(page)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := uint64(i%int(fastWritePagesPerChunk)) * fastWritePageSize
		if errno := fastWrite.Write(40, meta.ChunkSize, off, page, remote); errno != 0 {
			b.Fatalf("Fast Write write: %s", errno)
		}
	}
	b.StopTimer()
	if errno := fastWrite.Checkpoint(40); errno != 0 {
		b.Fatalf("Fast Write flush: %s", errno)
	}
}

func BenchmarkFastWriteWritePathMatrix(b *testing.B) {
	page := bytes.Repeat([]byte{'w'}, int(fastWritePageSize))
	small := page[:512]

	b.Run("XFSAligned4K", func(b *testing.B) {
		file, err := os.OpenFile(filepath.Join(b.TempDir(), "xfs.data"), os.O_CREATE|os.O_RDWR, 0600)
		require.NoError(b, err)
		defer file.Close()
		require.NoError(b, file.Truncate(meta.ChunkSize))
		b.SetBytes(int64(len(page)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := int64(i%int(fastWritePagesPerChunk)) * int64(fastWritePageSize)
			if _, err = file.WriteAt(page, off); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		require.NoError(b, file.Sync())
	})

	b.Run("FastWriteAligned4K", func(b *testing.B) {
		fastWrite, err := openFastWrite(b.TempDir())
		require.NoError(b, err)
		defer fastWrite.close()
		b.SetBytes(int64(len(page)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := uint64(i%int(fastWritePagesPerChunk)) * fastWritePageSize
			if errno := fastWrite.Write(60, meta.ChunkSize, off, page, fastWriteTestRemote(nil)); errno != 0 {
				b.Fatalf("Fast Write write: %s", errno)
			}
		}
		b.StopTimer()
		require.Zero(b, fastWrite.Checkpoint(60))
	})

	b.Run("XFSUnaligned512BRMW", func(b *testing.B) {
		file, err := os.OpenFile(filepath.Join(b.TempDir(), "xfs.data"), os.O_CREATE|os.O_RDWR, 0600)
		require.NoError(b, err)
		defer file.Close()
		require.NoError(b, file.Truncate(meta.ChunkSize))
		b.SetBytes(int64(len(small)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := int64((uint64(i)*7919 + 123) % uint64(meta.ChunkSize-len(small)))
			if _, err = file.WriteAt(small, off); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		require.NoError(b, file.Sync())
	})

	b.Run("FastWriteUnaligned512BRMW", func(b *testing.B) {
		fastWrite, err := openFastWrite(b.TempDir())
		require.NoError(b, err)
		defer fastWrite.close()
		remote := func(off uint64, dst []byte) (int, syscall.Errno) {
			for index := range dst {
				dst[index] = byte((off + uint64(index)) % 251)
			}
			return len(dst), 0
		}
		b.SetBytes(int64(len(small)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := (uint64(i)*7919 + 123) % uint64(meta.ChunkSize-len(small))
			if errno := fastWrite.Write(61, meta.ChunkSize, off, small, remote); errno != 0 {
				b.Fatalf("Fast Write write: %s", errno)
			}
		}
		b.StopTimer()
		require.Zero(b, fastWrite.Checkpoint(61))
	})
}

func BenchmarkFastWriteParallelInodes(b *testing.B) {
	fastWrite, err := openFastWrite(b.TempDir())
	require.NoError(b, err)
	defer fastWrite.close()
	page := bytes.Repeat([]byte{'p'}, int(fastWritePageSize))
	var nextInode atomic.Uint64
	b.SetBytes(int64(len(page)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		inode := Ino(nextInode.Add(1) + 1000)
		iteration := 0
		for pb.Next() {
			off := uint64(iteration%int(fastWritePagesPerChunk)) * fastWritePageSize
			if errno := fastWrite.Write(inode, meta.ChunkSize, off, page, fastWriteTestRemote(nil)); errno != 0 {
				b.Errorf("Fast Write write inode %d: %s", inode, errno)
				return
			}
			iteration++
		}
	})
	b.StopTimer()
	for _, inode := range fastWrite.listInodes() {
		if errno := fastWrite.Checkpoint(inode); errno != 0 {
			b.Fatalf("Fast Write checkpoint inode %d: %s", inode, errno)
		}
	}
}

func BenchmarkFastWriteReadPathMatrix(b *testing.B) {
	const dataSize = 4 << 20
	page := bytes.Repeat([]byte{'r'}, int(fastWritePageSize))

	b.Run("XFSLocal4K", func(b *testing.B) {
		file, err := os.OpenFile(filepath.Join(b.TempDir(), "xfs.data"), os.O_CREATE|os.O_RDWR, 0600)
		require.NoError(b, err)
		defer file.Close()
		for off := int64(0); off < dataSize; off += int64(fastWritePageSize) {
			_, err = file.WriteAt(page, off)
			require.NoError(b, err)
		}
		dst := make([]byte, fastWritePageSize)
		b.SetBytes(int64(len(dst)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := int64(i%(dataSize/int(fastWritePageSize))) * int64(fastWritePageSize)
			if _, err = file.ReadAt(dst, off); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("FastWriteLocal4K", func(b *testing.B) {
		fastWrite, err := openFastWrite(b.TempDir())
		require.NoError(b, err)
		defer fastWrite.close()
		for off := uint64(0); off < dataSize; off += fastWritePageSize {
			require.Zero(b, fastWrite.Write(62, dataSize, off, page, fastWriteTestRemote(nil)))
		}
		dst := make([]byte, fastWritePageSize)
		b.SetBytes(int64(len(dst)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := uint64(i%(dataSize/int(fastWritePageSize))) * fastWritePageSize
			if _, errno := fastWrite.Read(62, off, dst, fastWriteTestRemote(nil)); errno != 0 {
				b.Fatalf("Fast Write read: %s", errno)
			}
		}
	})

	b.Run("FastWriteRemoteFallback4K", func(b *testing.B) {
		fastWrite, err := openFastWrite(b.TempDir())
		require.NoError(b, err)
		defer fastWrite.close()
		remote := fastWriteTestRemote(bytes.Repeat([]byte{'r'}, dataSize))
		dst := make([]byte, fastWritePageSize)
		b.SetBytes(int64(len(dst)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := uint64(i%(dataSize/int(fastWritePageSize))) * fastWritePageSize
			if _, errno := fastWrite.Read(63, off, dst, remote); errno != 0 {
				b.Fatalf("Fast Write read: %s", errno)
			}
		}
	})
}

func BenchmarkFastWriteWriteAndCheckpoint(b *testing.B) {
	for _, chunks := range []int{1, 16} {
		b.Run(fmt.Sprintf("%dChunks", chunks), func(b *testing.B) {
			fastWrite, err := openFastWrite(b.TempDir())
			require.NoError(b, err)
			defer fastWrite.close()
			page := bytes.Repeat([]byte{'s'}, int(fastWritePageSize))
			length := uint64(chunks * meta.ChunkSize)
			b.SetBytes(int64(chunks) * int64(len(page)))
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				for chunkIndex := 0; chunkIndex < chunks; chunkIndex++ {
					off := uint64(chunkIndex)*meta.ChunkSize +
						uint64(iteration%int(fastWritePagesPerChunk))*fastWritePageSize
					if errno := fastWrite.Write(64, length, off, page, fastWriteTestRemote(nil)); errno != 0 {
						b.Fatalf("Fast Write write: %s", errno)
					}
				}
				if errno := fastWrite.Checkpoint(64); errno != 0 {
					b.Fatalf("Fast Write checkpoint: %s", errno)
				}
			}
		})
	}
}
