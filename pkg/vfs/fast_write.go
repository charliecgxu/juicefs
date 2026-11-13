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
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"golang.org/x/sys/unix"
)

const (
	fastWriteFormatVersion = 1
	fastWritePageSize      = uint64(4096)
	fastWritePagesPerChunk = uint32(meta.ChunkSize / fastWritePageSize)
	fastWriteBitmapSize    = fastWritePagesPerChunk / 8
	fastWriteFooterSize    = uint64(4096)
	fastWriteChunkFileSize = uint64(meta.ChunkSize) + fastWriteFooterSize
	fastWriteFDLimit       = 1024

	fastWriteActive  = "active"
	fastWriteSealed  = "sealed"
	fastWriteGarbage = ".gc"
	fastWriteSealing = ".sealing"
	fastWriteRetired = ".retired"
)

var (
	fastWriteLockMagic   = [8]byte{'J', 'F', 'S', 'F', 'W', 'R', '1', 0}
	fastWriteFooterMagic = [8]byte{'J', 'F', 'S', 'F', 'W', 'C', '1', 0}
)

type fastWriteChunk struct {
	Index  uint32
	Bitmap []byte
}

type fastWriteGeneration struct {
	ID           uint64
	State        string
	Length       uint64
	CheckpointNS int64
	Chunks       map[uint32]*fastWriteChunk
}

type fastWriteUploadRun struct {
	Chunk     uint32
	StartPage uint32
	EndPage   uint32
}

type fastWriteUploadGeneration struct {
	ID     uint64
	Length uint64
	Runs   []fastWriteUploadRun
}

type fastWriteFile struct {
	sync.RWMutex
	mutationMu        sync.Mutex
	uploadMu          sync.Mutex
	inode             Ino
	length            uint64
	nextGeneration    uint64
	generations       []*fastWriteGeneration
	checkpointPending bool
	sealPending       bool
	metadataDirty     bool
	metadataEpoch     uint64
	deleted           bool
}

type fastWriteFDKey struct {
	inode Ino
	gen   uint64
	chunk uint32
}

type fastWriteFDEntry struct {
	key  fastWriteFDKey
	file *os.File
	elem *list.Element
	refs int
}

type fastWrite struct {
	root     string
	lockFile *os.File
	volumeID string

	sync.RWMutex
	files map[Ino]*fastWriteFile

	fdMu        sync.Mutex
	fds         map[fastWriteFDKey]*fastWriteFDEntry
	fdLRU       *list.List
	fdLimit     int
	dirtyFD     map[fastWriteFDKey]struct{}
	dirtyChunks map[fastWriteFDKey]struct{}
	syncSlots   chan struct{}

	syncFileHook   func(*os.File) error
	syncDirHook    func(string) error
	writeFileHook  func(*os.File, []byte, int64) (int, error)
	removeAllHook  func(string) error
	cloneRangeHook func(dstFD, srcFD int, srcOff, dstOff, length uint64) error
}

type fastWriteRemoteReader func(off uint64, dst []byte) (int, syscall.Errno)

type fastWriteWriteGuard struct {
	file            *fastWriteFile
	generation      uint64
	originalLength  uint64
	releaseMutation bool
}

type fastWriteMutationGuard struct {
	file           *fastWriteFile
	originalLength uint64
}

func (g *fastWriteWriteGuard) release() {
	if g == nil || g.file == nil {
		return
	}
	file := g.file
	releaseMutation := g.releaseMutation
	g.file = nil
	file.Unlock()
	if releaseMutation {
		file.mutationMu.Unlock()
	}
}

func (g *fastWriteMutationGuard) release() {
	if g == nil || g.file == nil {
		return
	}
	file := g.file
	g.file = nil
	file.mutationMu.Unlock()
}

func openFastWrite(root string, volume ...string) (*fastWrite, error) {
	if root == "" {
		return nil, nil
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve Fast Write directory: %w", err)
	}
	if err = os.MkdirAll(filepath.Join(abs, "inodes"), 0700); err != nil {
		return nil, fmt.Errorf("create Fast Write directory: %w", err)
	}
	lockFile, err := os.OpenFile(filepath.Join(abs, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open Fast Write lock: %w", err)
	}
	if err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("Fast Write directory is already in use: %w", err)
	}
	o := &fastWrite{
		root: abs, lockFile: lockFile, files: make(map[Ino]*fastWriteFile),
		fds: make(map[fastWriteFDKey]*fastWriteFDEntry), fdLRU: list.New(),
		fdLimit: fastWriteFDLimit, dirtyFD: make(map[fastWriteFDKey]struct{}),
		dirtyChunks: make(map[fastWriteFDKey]struct{}),
		syncSlots:   make(chan struct{}, DefaultFastWriteSyncConcurrency),
	}
	if len(volume) > 0 {
		o.volumeID = volume[0]
	}
	if err = o.initLockHeader(); err == nil {
		err = syncDir(filepath.Join(abs, "inodes"))
	}
	if err == nil {
		err = syncDir(abs)
	}
	if err == nil && filepath.Dir(abs) != abs {
		err = syncDir(filepath.Dir(abs))
	}
	if err == nil {
		err = o.recover()
	}
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return nil, err
	}
	return o, nil
}

func (o *fastWrite) initLockHeader() error {
	const size = 64
	info, err := o.lockFile.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		if len(o.volumeID) > size-16 {
			return errors.New("Fast Write volume UUID is too long")
		}
		buf := make([]byte, size)
		copy(buf, fastWriteLockMagic[:])
		binary.LittleEndian.PutUint32(buf[8:12], fastWriteFormatVersion)
		binary.LittleEndian.PutUint32(buf[12:16], uint32(len(o.volumeID)))
		copy(buf[16:], o.volumeID)
		if err = writeFullAt(o.lockFile, buf, 0); err != nil {
			return err
		}
		return o.lockFile.Sync()
	}
	if info.Size() != size {
		return fmt.Errorf("invalid Fast Write header size %d", info.Size())
	}
	buf := make([]byte, size)
	if err = readExactAt(o.lockFile, buf, 0); err != nil {
		return fmt.Errorf("read Fast Write header: %w", err)
	}
	if string(buf[:8]) != string(fastWriteLockMagic[:]) ||
		binary.LittleEndian.Uint32(buf[8:12]) != fastWriteFormatVersion {
		return errors.New("unsupported Fast Write format (expected v1)")
	}
	n := binary.LittleEndian.Uint32(buf[12:16])
	if n > size-16 {
		return errors.New("invalid Fast Write volume UUID")
	}
	stored := string(buf[16 : 16+n])
	if o.volumeID != "" && stored != o.volumeID {
		return fmt.Errorf("Fast Write belongs to volume %q, not %q", stored, o.volumeID)
	}
	return nil
}

func (o *fastWrite) close() error {
	if o == nil || o.lockFile == nil {
		return nil
	}
	o.closeAllFDs()
	var clean []Ino
	o.RLock()
	for inode, file := range o.files {
		file.RLock()
		if file.deleted || fastWriteFileCleanLocked(file) {
			clean = append(clean, inode)
		}
		file.RUnlock()
	}
	o.RUnlock()
	var cleanupErr error
	for _, inode := range clean {
		if err := o.remove(inode); err != nil && cleanupErr == nil {
			cleanupErr = err
		}
	}
	if cleanupErr != nil {
		_ = o.abort()
		return cleanupErr
	}
	return o.abort()
}

func (o *fastWrite) abort() error {
	if o == nil || o.lockFile == nil {
		return nil
	}
	o.closeAllFDs()
	_ = syscall.Flock(int(o.lockFile.Fd()), syscall.LOCK_UN)
	err := o.lockFile.Close()
	o.lockFile = nil
	return err
}

func (o *fastWrite) inodeDir(inode Ino) string {
	return filepath.Join(o.root, "inodes", fmt.Sprintf("%d", inode))
}

func generationDirName(state string, generation uint64) string {
	return fmt.Sprintf("%s-%020d", state, generation)
}

func garbageGenerationDirName(generation uint64) string {
	return generationDirName(fastWriteGarbage, generation)
}

func (o *fastWrite) generationDir(inode Ino, state string, generation uint64) string {
	return filepath.Join(o.inodeDir(inode), generationDirName(state, generation))
}

func (o *fastWrite) garbageGenerationDir(inode Ino, generation uint64) string {
	return filepath.Join(o.inodeDir(inode), garbageGenerationDirName(generation))
}

func transientGenerationDirName(state string, generation uint64) string {
	return generationDirName(state, generation)
}

func (o *fastWrite) transientGenerationDir(inode Ino, state string, generation uint64) string {
	return filepath.Join(o.inodeDir(inode), transientGenerationDirName(state, generation))
}

func chunkFileName(index uint32) string {
	return fmt.Sprintf("chunk-%08x.data", index)
}

func (o *fastWrite) dataPath(inode Ino, generation uint64, index uint32) string {
	name := chunkFileName(index)
	active := filepath.Join(o.generationDir(inode, fastWriteActive, generation), name)
	if _, err := os.Stat(active); err == nil {
		return active
	}
	return filepath.Join(o.generationDir(inode, fastWriteSealed, generation), name)
}

func parseGenerationDir(name string) (string, uint64, bool) {
	for _, state := range []string{fastWriteActive, fastWriteSealed} {
		prefix := state + "-"
		if strings.HasPrefix(name, prefix) && len(name) == len(prefix)+20 {
			id, err := strconv.ParseUint(strings.TrimPrefix(name, prefix), 10, 64)
			return state, id, err == nil && id != 0 && name == generationDirName(state, id)
		}
	}
	return "", 0, false
}

func parseGarbageGenerationDir(name string) (uint64, bool) {
	prefix := fastWriteGarbage + "-"
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+20 {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(name, prefix), 10, 64)
	return id, err == nil && id != 0 && name == garbageGenerationDirName(id)
}

func parseTransientGenerationDir(name, state string) (uint64, bool) {
	prefix := state + "-"
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+20 {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(name, prefix), 10, 64)
	return id, err == nil && id != 0 && name == transientGenerationDirName(state, id)
}

func parseChunkName(name string) (uint32, bool) {
	if !strings.HasPrefix(name, "chunk-") || !strings.HasSuffix(name, ".data") {
		return 0, false
	}
	value := strings.TrimSuffix(strings.TrimPrefix(name, "chunk-"), ".data")
	if len(value) != 8 {
		return 0, false
	}
	index, err := strconv.ParseUint(value, 16, 32)
	return uint32(index), err == nil && name == fmt.Sprintf("chunk-%08x.data", index)
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func generationDirectoriesMatch(first, second string, generation uint64) (bool, error) {
	firstEntries, err := os.ReadDir(first)
	if err != nil {
		return false, err
	}
	secondEntries, err := os.ReadDir(second)
	if err != nil {
		return false, err
	}
	if len(firstEntries) != len(secondEntries) {
		return false, nil
	}
	for entryIndex, firstEntry := range firstEntries {
		secondEntry := secondEntries[entryIndex]
		if firstEntry.Name() != secondEntry.Name() || firstEntry.IsDir() || secondEntry.IsDir() {
			return false, nil
		}
		chunkIndex, ok := parseChunkName(firstEntry.Name())
		if !ok {
			return false, nil
		}
		firstPath := filepath.Join(first, firstEntry.Name())
		secondPath := filepath.Join(second, secondEntry.Name())
		firstChunk, firstLength, firstCheckpoint, readErr := readChunkFooter(firstPath, generation, chunkIndex)
		if readErr != nil {
			return false, readErr
		}
		secondChunk, secondLength, secondCheckpoint, readErr := readChunkFooter(secondPath, generation, chunkIndex)
		if readErr != nil {
			return false, readErr
		}
		if firstLength != secondLength || firstCheckpoint != secondCheckpoint ||
			!bytes.Equal(firstChunk.Bitmap, secondChunk.Bitmap) {
			return false, nil
		}
		firstFile, openErr := os.Open(firstPath)
		if openErr != nil {
			return false, openErr
		}
		secondFile, openErr := os.Open(secondPath)
		if openErr != nil {
			_ = firstFile.Close()
			return false, openErr
		}
		firstPage := make([]byte, fastWritePageSize)
		secondPage := make([]byte, fastWritePageSize)
		matches := true
		for page := uint32(0); page < fastWritePagesPerChunk && matches; page++ {
			if !bitmapGet(firstChunk.Bitmap, page) {
				continue
			}
			offset := int64(uint64(page) * fastWritePageSize)
			if readErr = readExactAt(firstFile, firstPage, offset); readErr == nil {
				readErr = readExactAt(secondFile, secondPage, offset)
			}
			if readErr != nil || !bytes.Equal(firstPage, secondPage) {
				matches = false
			}
		}
		firstCloseErr := firstFile.Close()
		secondCloseErr := secondFile.Close()
		for _, checkErr := range []error{readErr, firstCloseErr, secondCloseErr} {
			if checkErr != nil {
				return false, checkErr
			}
		}
		if !matches {
			return false, nil
		}
	}
	return true, nil
}

func (o *fastWrite) recoverSealTransitions(inode Ino) error {
	inodeDir := o.inodeDir(inode)
	changed := false
	entries, err := os.ReadDir(inodeDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		id, sealing := parseTransientGenerationDir(entry.Name(), fastWriteSealing)
		if !sealing {
			continue
		}
		if !entry.IsDir() {
			return fmt.Errorf("invalid Fast Write sealing entry %q", entry.Name())
		}
		active, activeErr := pathExists(o.generationDir(inode, fastWriteActive, id))
		sealed, sealedErr := pathExists(o.generationDir(inode, fastWriteSealed, id))
		retired, retiredErr := pathExists(o.transientGenerationDir(inode, fastWriteRetired, id))
		if activeErr != nil || sealedErr != nil || retiredErr != nil {
			return fmt.Errorf("inspect Fast Write sealing generation %d", id)
		}
		if !active && !sealed && !retired {
			return fmt.Errorf("orphan Fast Write sealing generation %d", id)
		}
		if err = o.removeAll(o.transientGenerationDir(inode, fastWriteSealing, id)); err != nil {
			return fmt.Errorf("remove Fast Write sealing generation %d: %w", id, err)
		}
		changed = true
	}

	entries, err = os.ReadDir(inodeDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		id, retired := parseTransientGenerationDir(entry.Name(), fastWriteRetired)
		if !retired {
			continue
		}
		if !entry.IsDir() {
			return fmt.Errorf("invalid Fast Write retired entry %q", entry.Name())
		}
		activePath := o.generationDir(inode, fastWriteActive, id)
		sealedPath := o.generationDir(inode, fastWriteSealed, id)
		active, activeErr := pathExists(activePath)
		sealed, sealedErr := pathExists(sealedPath)
		if activeErr != nil || sealedErr != nil {
			return fmt.Errorf("inspect Fast Write retired generation %d", id)
		}
		retiredPath := o.transientGenerationDir(inode, fastWriteRetired, id)
		if active || sealed {
			if err = o.removeAll(retiredPath); err != nil {
				return fmt.Errorf("remove Fast Write retired generation %d: %w", id, err)
			}
		} else if err = os.Rename(retiredPath, activePath); err != nil {
			return fmt.Errorf("restore Fast Write active generation %d: %w", id, err)
		}
		changed = true
	}

	entries, err = os.ReadDir(inodeDir)
	if err != nil {
		return err
	}
	active := make(map[uint64]bool)
	sealed := make(map[uint64]bool)
	for _, entry := range entries {
		state, id, ok := parseGenerationDir(entry.Name())
		if !ok {
			continue
		}
		if state == fastWriteActive {
			active[id] = true
		} else {
			sealed[id] = true
		}
	}
	for id := range active {
		if !sealed[id] {
			continue
		}
		matches, matchErr := generationDirectoriesMatch(
			o.generationDir(inode, fastWriteActive, id),
			o.generationDir(inode, fastWriteSealed, id), id)
		if matchErr != nil {
			return fmt.Errorf("compare duplicate Fast Write generation %d: %w", id, matchErr)
		}
		if !matches {
			return fmt.Errorf("duplicate Fast Write generation %d is not a completed seal clone", id)
		}
		if err = o.removeAll(o.generationDir(inode, fastWriteSealed, id)); err != nil {
			return fmt.Errorf("remove duplicate Fast Write sealed generation %d: %w", id, err)
		}
		changed = true
	}
	if changed {
		return o.syncDirectory(inodeDir)
	}
	return nil
}

func (o *fastWrite) recover() error {
	entries, err := os.ReadDir(filepath.Join(o.root, "inodes"))
	if err != nil {
		return fmt.Errorf("scan Fast Write: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return fmt.Errorf("invalid Fast Write inode entry %q", entry.Name())
		}
		inodeValue, parseErr := strconv.ParseUint(entry.Name(), 10, 64)
		if parseErr != nil || inodeValue == 0 || entry.Name() != strconv.FormatUint(inodeValue, 10) {
			return fmt.Errorf("invalid Fast Write inode directory %q", entry.Name())
		}
		inode := Ino(inodeValue)
		inodeDir := o.inodeDir(inode)
		if transitionErr := o.recoverSealTransitions(inode); transitionErr != nil {
			return fmt.Errorf("recover Fast Write seal for inode %d: %w", inode, transitionErr)
		}
		deleted := false
		if _, statErr := os.Stat(filepath.Join(inodeDir, ".deleted")); statErr == nil {
			deleted = true
		} else if !os.IsNotExist(statErr) {
			return fmt.Errorf("inspect Fast Write deleted marker for inode %d: %w", inode, statErr)
		}
		file := &fastWriteFile{inode: inode, nextGeneration: 1, deleted: deleted}
		genEntries, readErr := os.ReadDir(inodeDir)
		if readErr != nil {
			return readErr
		}
		activeCount := 0
		seenGenerations := make(map[uint64]struct{})
		for _, genEntry := range genEntries {
			if genEntry.Name() == ".deleted" {
				continue
			}
			if id, garbage := parseGarbageGenerationDir(genEntry.Name()); garbage {
				if cleanupErr := o.removeAll(o.garbageGenerationDir(inode, id)); cleanupErr != nil {
					logger.Warnf("clean Fast Write uploaded generation %d for inode %d: %s", id, inode, cleanupErr)
				}
				continue
			}
			if !genEntry.IsDir() {
				return fmt.Errorf("invalid Fast Write inode entry %q", genEntry.Name())
			}
			state, id, ok := parseGenerationDir(genEntry.Name())
			if !ok {
				return fmt.Errorf("invalid Fast Write generation directory %q", genEntry.Name())
			}
			if _, exists := seenGenerations[id]; exists {
				return fmt.Errorf("duplicate Fast Write generation %d", id)
			}
			seenGenerations[id] = struct{}{}
			generation := &fastWriteGeneration{ID: id, State: state, Chunks: make(map[uint32]*fastWriteChunk)}
			if state == fastWriteActive {
				activeCount++
			}
			if id >= file.nextGeneration {
				file.nextGeneration = id + 1
			}
			chunkEntries, readErr := os.ReadDir(filepath.Join(inodeDir, genEntry.Name()))
			if readErr != nil {
				return readErr
			}
			for _, chunkEntry := range chunkEntries {
				if chunkEntry.IsDir() {
					return fmt.Errorf("unexpected directory %q in Fast Write generation", chunkEntry.Name())
				}
				index, ok := parseChunkName(chunkEntry.Name())
				if !ok {
					return fmt.Errorf("invalid Fast Write chunk %q", chunkEntry.Name())
				}
				path := filepath.Join(inodeDir, genEntry.Name(), chunkEntry.Name())
				chunk, length, checkpointNS, footerErr := readChunkFooter(path, id, index)
				if footerErr != nil {
					return fmt.Errorf("recover Fast Write chunk %s: %w", path, footerErr)
				}
				// A chunk without a checkpointed footer only contains writes
				// for which fsync never completed. Ignore it and fall back to
				// the remote slice view after recovery.
				if bitmapEmpty(chunk.Bitmap) && checkpointNS == 0 {
					continue
				}
				generation.Chunks[index] = chunk
				if length > generation.Length {
					generation.Length = length
				}
				if checkpointNS > generation.CheckpointNS {
					generation.CheckpointNS = checkpointNS
				}
			}
			file.generations = append(file.generations, generation)
		}
		if activeCount > 1 {
			return fmt.Errorf("Fast Write inode %d has %d active generations", inode, activeCount)
		}
		hasCheckpoint := false
		for _, generation := range file.generations {
			if len(generation.Chunks) != 0 {
				hasCheckpoint = true
				break
			}
		}
		if !hasCheckpoint {
			if err = os.RemoveAll(inodeDir); err != nil {
				return err
			}
			continue
		}
		sort.Slice(file.generations, func(i, j int) bool { return file.generations[i].ID < file.generations[j].ID })
		for _, generation := range file.generations {
			if generation.CheckpointNS != 0 {
				file.length = generation.Length
			}
		}
		if activeCount == 0 {
			generation := &fastWriteGeneration{ID: file.nextGeneration, State: fastWriteActive, Chunks: make(map[uint32]*fastWriteChunk)}
			file.nextGeneration++
			if err = os.Mkdir(o.generationDir(inode, fastWriteActive, generation.ID), 0700); err != nil {
				return err
			}
			file.generations = append(file.generations, generation)
		}
		o.files[inode] = file
	}
	return nil
}

func (o *fastWrite) reconcileMetadata(m meta.Meta, keepUnlinked func(Ino) bool) error {
	o.RLock()
	inodes := make([]Ino, 0, len(o.files))
	for inode := range o.files {
		inodes = append(inodes, inode)
	}
	o.RUnlock()
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })
	for _, inode := range inodes {
		var attr meta.Attr
		errno := m.GetAttr(meta.Background(), inode, &attr)
		if errno == syscall.ENOENT || errno == 0 && attr.Nlink == 0 {
			if keepUnlinked != nil && keepUnlinked(inode) {
				if !o.isDeleted(inode) {
					if err := o.markDeleted(inode); err != nil {
						return fmt.Errorf("preserve unlinked Fast Write inode %d: %w", inode, err)
					}
				}
				continue
			}
			if err := o.remove(inode); err != nil {
				return fmt.Errorf("remove orphan Fast Write inode %d: %w", inode, err)
			}
			continue
		}
		if errno != 0 {
			return fmt.Errorf("reconcile Fast Write inode %d: %s", inode, errno)
		}
		if file := o.get(inode); file != nil {
			growFastWriteFile(file, attr.Length)
			if o.isDeleted(inode) {
				if err := o.unmarkDeleted(inode); err != nil {
					return fmt.Errorf("restore linked Fast Write inode %d: %w", inode, err)
				}
			}
		}
	}
	return nil
}

func readChunkFooter(path string, generation uint64, index uint32) (*fastWriteChunk, uint64, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, 0, 0, err
	}
	if info.Size() != int64(fastWriteChunkFileSize) {
		return nil, 0, 0, fmt.Errorf("invalid chunk size %d", info.Size())
	}
	footer := make([]byte, fastWriteFooterSize)
	if err = readExactAt(file, footer, int64(meta.ChunkSize)); err != nil {
		return nil, 0, 0, err
	}
	if allZero(footer) {
		return &fastWriteChunk{Index: index, Bitmap: make([]byte, fastWriteBitmapSize)}, 0, 0, nil
	}
	if string(footer[2048:2056]) != string(fastWriteFooterMagic[:]) ||
		binary.LittleEndian.Uint32(footer[2056:2060]) != fastWriteFormatVersion ||
		binary.LittleEndian.Uint64(footer[2064:2072]) != generation ||
		binary.LittleEndian.Uint32(footer[2072:2076]) != index {
		return nil, 0, 0, errors.New("invalid footer identity")
	}
	expected := binary.LittleEndian.Uint32(footer[4092:])
	if crc32.ChecksumIEEE(footer[:4092]) != expected {
		return nil, 0, 0, errors.New("footer checksum mismatch")
	}
	bitmap := append([]byte(nil), footer[:fastWriteBitmapSize]...)
	return &fastWriteChunk{Index: index, Bitmap: bitmap},
		binary.LittleEndian.Uint64(footer[2080:2088]),
		int64(binary.LittleEndian.Uint64(footer[2088:2096])), nil
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func (o *fastWrite) get(inode Ino) *fastWriteFile {
	o.RLock()
	file := o.files[inode]
	o.RUnlock()
	return file
}

func (o *fastWrite) lockUpload(inode Ino) func() {
	for {
		file := o.get(inode)
		if file == nil {
			return func() {}
		}
		file.uploadMu.Lock()
		if o.get(inode) == file {
			return file.uploadMu.Unlock
		}
		file.uploadMu.Unlock()
	}
}

func (o *fastWrite) getOrCreate(inode Ino, remoteLength uint64) (*fastWriteFile, error) {
	if file := o.get(inode); file != nil {
		return file, nil
	}
	o.Lock()
	defer o.Unlock()
	if file := o.files[inode]; file != nil {
		return file, nil
	}
	if err := os.MkdirAll(o.inodeDir(inode), 0700); err != nil {
		return nil, err
	}
	generation := &fastWriteGeneration{ID: 1, State: fastWriteActive, Chunks: make(map[uint32]*fastWriteChunk)}
	if err := os.MkdirAll(o.generationDir(inode, fastWriteActive, 1), 0700); err != nil {
		return nil, err
	}
	file := &fastWriteFile{
		inode: inode, length: remoteLength,
		nextGeneration: 2, generations: []*fastWriteGeneration{generation},
	}
	o.files[inode] = file
	return file, nil
}

func (o *fastWrite) lockMutation(inode Ino, remoteLength uint64) (*fastWriteMutationGuard, syscall.Errno) {
	for {
		file, err := o.getOrCreate(inode, remoteLength)
		if err != nil {
			return nil, syscall.EIO
		}
		file.mutationMu.Lock()
		if o.get(inode) != file {
			file.mutationMu.Unlock()
			continue
		}
		file.Lock()
		growFastWriteFileLocked(file, remoteLength)
		length := file.length
		file.Unlock()
		return &fastWriteMutationGuard{file: file, originalLength: length}, 0
	}
}

func (o *fastWrite) lockExistingMutation(inode Ino) *fastWriteMutationGuard {
	for {
		file := o.get(inode)
		if file == nil {
			return nil
		}
		file.mutationMu.Lock()
		if o.get(inode) != file {
			file.mutationMu.Unlock()
			continue
		}
		file.RLock()
		length := file.length
		file.RUnlock()
		return &fastWriteMutationGuard{file: file, originalLength: length}
	}
}

func growFastWriteFileLocked(file *fastWriteFile, remoteLength uint64) {
	if remoteLength > file.length {
		file.length = remoteLength
	}
}

func growFastWriteFile(file *fastWriteFile, remoteLength uint64) {
	file.Lock()
	growFastWriteFileLocked(file, remoteLength)
	file.Unlock()
}

func (o *fastWrite) growRemoteLength(inode Ino, remoteLength uint64) {
	if file := o.get(inode); file != nil {
		growFastWriteFile(file, remoteLength)
	}
}

func activeGeneration(file *fastWriteFile) *fastWriteGeneration {
	for i := len(file.generations) - 1; i >= 0; i-- {
		if file.generations[i].State == fastWriteActive {
			return file.generations[i]
		}
	}
	return nil
}

func ensureFastWriteChunk(generation *fastWriteGeneration, index uint32) *fastWriteChunk {
	chunk := generation.Chunks[index]
	if chunk == nil {
		chunk = &fastWriteChunk{Index: index, Bitmap: make([]byte, fastWriteBitmapSize)}
		generation.Chunks[index] = chunk
	}
	return chunk
}

func bitmapGet(bitmap []byte, page uint32) bool {
	return bitmap[page>>3]&(1<<(page&7)) != 0
}

func bitmapSet(bitmap []byte, page uint32) {
	bitmap[page>>3] |= 1 << (page & 7)
}

func bitmapEmpty(bitmap []byte) bool {
	return allZero(bitmap)
}

func bitmapRuns(bitmap []byte) [][2]uint32 {
	var runs [][2]uint32
	var start uint32
	inRun := false
	for page := uint32(0); page < fastWritePagesPerChunk; page++ {
		set := bitmapGet(bitmap, page)
		if set && !inRun {
			start, inRun = page, true
		} else if !set && inRun {
			runs = append(runs, [2]uint32{start, page})
			inRun = false
		}
	}
	if inRun {
		runs = append(runs, [2]uint32{start, fastWritePagesPerChunk})
	}
	return runs
}

func writeFullAt(file *os.File, data []byte, off int64) error {
	_, err := writeFullAtCount(file, data, off)
	return err
}

func writeFullAtCount(file *os.File, data []byte, off int64) (int, error) {
	written := 0
	for len(data) > 0 {
		n, err := file.WriteAt(data, off)
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
		data, off = data[n:], off+int64(n)
	}
	return written, nil
}

func readExactAt(file *os.File, data []byte, off int64) error {
	for len(data) > 0 {
		n, err := file.ReadAt(data, off)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		data, off = data[n:], off+int64(n)
	}
	return nil
}

func readFullAt(file *os.File, data []byte, off int64) error {
	for len(data) > 0 {
		n, err := file.ReadAt(data, off)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			clear(data)
			return nil
		}
		data, off = data[n:], off+int64(n)
		if err == io.EOF {
			clear(data)
			return nil
		}
	}
	return nil
}

func (o *fastWrite) writeFile(file *os.File, data []byte, off int64) (int, error) {
	if o.writeFileHook != nil {
		return o.writeFileHook(file, data, off)
	}
	return writeFullAtCount(file, data, off)
}

func (o *fastWrite) removeAll(path string) error {
	if o.removeAllHook != nil {
		return o.removeAllHook(path)
	}
	return os.RemoveAll(path)
}

func (o *fastWrite) setSyncConcurrency(concurrency int) {
	if concurrency <= 0 {
		concurrency = DefaultFastWriteSyncConcurrency
	}
	o.syncSlots = make(chan struct{}, concurrency)
}

func (o *fastWrite) withSyncSlot(syncFn func() error) error {
	o.syncSlots <- struct{}{}
	defer func() { <-o.syncSlots }()
	return syncFn()
}

func (o *fastWrite) syncFile(file *os.File) error {
	return o.withSyncSlot(func() error {
		if o.syncFileHook != nil {
			return o.syncFileHook(file)
		}
		return unix.Fdatasync(int(file.Fd()))
	})
}

func (o *fastWrite) syncDirectory(path string) error {
	if o.syncDirHook != nil {
		return o.syncDirHook(path)
	}
	return syncDir(path)
}

func (o *fastWrite) getFD(key fastWriteFDKey, create bool) (*os.File, error) {
	o.fdMu.Lock()
	defer o.fdMu.Unlock()
	if entry := o.fds[key]; entry != nil {
		o.fdLRU.MoveToFront(entry.elem)
		entry.refs++
		return entry.file, nil
	}
	path := o.dataPath(key.inode, key.gen, key.chunk)
	if create {
		path = filepath.Join(o.generationDir(key.inode, fastWriteActive, key.gen), fmt.Sprintf("chunk-%08x.data", key.chunk))
	}
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	if create {
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return nil, statErr
		}
		if info.Size() == 0 {
			if err = file.Truncate(int64(fastWriteChunkFileSize)); err != nil {
				_ = file.Close()
				return nil, err
			}
		} else if info.Size() != int64(fastWriteChunkFileSize) {
			_ = file.Close()
			return nil, fmt.Errorf("invalid Fast Write chunk size %d", info.Size())
		}
	}
	entry := &fastWriteFDEntry{key: key, file: file, refs: 1}
	entry.elem = o.fdLRU.PushFront(entry)
	o.fds[key] = entry
	for len(o.fds) > o.fdLimit {
		oldest := o.fdLRU.Back()
		for oldest != nil && oldest.Value.(*fastWriteFDEntry).refs != 0 {
			oldest = oldest.Prev()
		}
		if oldest == nil {
			break
		}
		victim := oldest.Value.(*fastWriteFDEntry)
		if _, dirty := o.dirtyFD[victim.key]; dirty {
			if err = o.syncFile(victim.file); err != nil {
				delete(o.fds, key)
				o.fdLRU.Remove(entry.elem)
				_ = file.Close()
				return nil, err
			}
			delete(o.dirtyFD, victim.key)
		}
		_ = victim.file.Close()
		delete(o.fds, victim.key)
		o.fdLRU.Remove(oldest)
	}
	return file, nil
}

func (o *fastWrite) releaseFD(key fastWriteFDKey) {
	o.fdMu.Lock()
	if entry := o.fds[key]; entry != nil && entry.refs > 0 {
		entry.refs--
	}
	o.fdMu.Unlock()
}

func (o *fastWrite) markFDDirty(key fastWriteFDKey) {
	o.fdMu.Lock()
	o.dirtyFD[key] = struct{}{}
	o.dirtyChunks[key] = struct{}{}
	o.fdMu.Unlock()
}

func (o *fastWrite) closeGenerationFDs(inode Ino, generation uint64) error {
	o.fdMu.Lock()
	defer o.fdMu.Unlock()
	for key, entry := range o.fds {
		if key.inode != inode || key.gen != generation {
			continue
		}
		if _, dirty := o.dirtyFD[key]; dirty {
			if err := o.syncFile(entry.file); err != nil {
				return err
			}
			delete(o.dirtyFD, key)
		}
		delete(o.dirtyChunks, key)
		if err := entry.file.Close(); err != nil {
			return err
		}
		o.fdLRU.Remove(entry.elem)
		delete(o.fds, key)
	}
	for key := range o.dirtyFD {
		if key.inode == inode && key.gen == generation {
			delete(o.dirtyFD, key)
		}
	}
	for key := range o.dirtyChunks {
		if key.inode == inode && key.gen == generation {
			delete(o.dirtyChunks, key)
		}
	}
	return nil
}

func (o *fastWrite) closeAllFDs() {
	o.fdMu.Lock()
	defer o.fdMu.Unlock()
	for _, entry := range o.fds {
		_ = entry.file.Close()
	}
	clear(o.fds)
	clear(o.dirtyFD)
	clear(o.dirtyChunks)
	o.fdLRU.Init()
}

func fastWritePageSourceLocked(file *fastWriteFile, chunkIndex, page uint32) (*fastWriteGeneration, *fastWriteChunk) {
	for i := len(file.generations) - 1; i >= 0; i-- {
		generation := file.generations[i]
		chunk := generation.Chunks[chunkIndex]
		if chunk != nil && bitmapGet(chunk.Bitmap, page) {
			return generation, chunk
		}
	}
	return nil, nil
}

func (o *fastWrite) readPageLocked(file *fastWriteFile, chunkIndex, page uint32, dst []byte, remote fastWriteRemoteReader) syscall.Errno {
	generation, _ := fastWritePageSourceLocked(file, chunkIndex, page)
	if generation != nil {
		key := fastWriteFDKey{file.inode, generation.ID, chunkIndex}
		dataFile, err := o.getFD(key, false)
		if err == nil {
			defer o.releaseFD(key)
		}
		if err != nil || readFullAt(dataFile, dst, int64(uint64(page)*fastWritePageSize)) != nil {
			return syscall.EIO
		}
		return 0
	}
	absolute := uint64(chunkIndex)*meta.ChunkSize + uint64(page)*fastWritePageSize
	n, errno := remote(absolute, dst)
	if errno == 0 {
		clear(dst[n:])
	}
	return errno
}

func markFastWriteMutationLocked(file *fastWriteFile) {
	file.checkpointPending = true
	file.sealPending = true
	markFastWriteMetadataDirtyLocked(file)
}

func markFastWriteMetadataDirtyLocked(file *fastWriteFile) {
	file.metadataDirty = true
	file.metadataEpoch++
}

func (o *fastWrite) beginWrite(inode Ino, remoteLength, off, length uint64) (*fastWriteWriteGuard, syscall.Errno) {
	if length == 0 || off+length < off {
		return nil, syscall.EINVAL
	}
	mutation, errno := o.lockMutation(inode, remoteLength)
	if errno != 0 {
		return nil, errno
	}
	guard, errno := o.beginWriteWithMutation(mutation, off, length, true)
	if errno != 0 {
		mutation.release()
	}
	return guard, errno
}

func (o *fastWrite) beginWriteWithMutation(mutation *fastWriteMutationGuard, off, length uint64,
	releaseMutation bool) (*fastWriteWriteGuard, syscall.Errno) {
	if mutation == nil || mutation.file == nil || length == 0 || off+length < off {
		return nil, syscall.EINVAL
	}
	file := mutation.file
	file.Lock()
	if err := o.prepareMutationLocked(file); err != nil {
		file.Unlock()
		return nil, syscall.EIO
	}
	generation := activeGeneration(file)
	if generation == nil {
		file.Unlock()
		return nil, syscall.EIO
	}
	return &fastWriteWriteGuard{
		file:            file,
		generation:      generation.ID,
		originalLength:  mutation.originalLength,
		releaseMutation: releaseMutation,
	}, 0
}

func (o *fastWrite) writeLocked(guard *fastWriteWriteGuard, off uint64, data []byte,
	remote fastWriteRemoteReader) (int, syscall.Errno) {
	file := guard.file
	generation := activeGeneration(file)
	if generation == nil || generation.ID != guard.generation {
		return 0, syscall.EIO
	}
	total := 0
	for len(data) > 0 {
		chunkIndex := uint32(off / meta.ChunkSize)
		chunkOff := off % meta.ChunkSize
		page := uint32(chunkOff / fastWritePageSize)
		pageOff := int(chunkOff % fastWritePageSize)
		chunk := ensureFastWriteChunk(generation, chunkIndex)
		key := fastWriteFDKey{file.inode, generation.ID, chunkIndex}
		dataFile, openErr := o.getFD(key, true)
		if openErr != nil {
			return total, syscall.EIO
		}
		if pageOff == 0 && len(data) >= int(fastWritePageSize) {
			pages := min(len(data)/int(fastWritePageSize), int(fastWritePagesPerChunk-page))
			n := pages * int(fastWritePageSize)
			written, writeErr := o.writeFile(dataFile, data[:n], int64(chunkOff))
			if written < 0 || written > n {
				written = max(0, min(written, n))
				writeErr = io.ErrShortWrite
			}
			if writeErr == nil && written != n {
				writeErr = io.ErrShortWrite
			}
			completedPages := written / int(fastWritePageSize)
			committed := completedPages * int(fastWritePageSize)
			if partial := written % int(fastWritePageSize); partial != 0 &&
				bitmapGet(chunk.Bitmap, page+uint32(completedPages)) {
				committed += partial
			}
			for candidate := page; candidate < page+uint32(completedPages); candidate++ {
				bitmapSet(chunk.Bitmap, candidate)
			}
			if committed > 0 {
				o.markFDDirty(key)
				completedEnd := off + uint64(committed)
				if completedEnd > file.length {
					file.length = completedEnd
				}
				markFastWriteMutationLocked(file)
			}
			total += committed
			o.releaseFD(key)
			if writeErr != nil {
				if errors.Is(writeErr, syscall.ENOSPC) {
					return total, syscall.ENOSPC
				}
				return total, syscall.EIO
			}
			off, data = off+uint64(n), data[n:]
			continue
		}
		n := min(len(data), int(fastWritePageSize)-pageOff)
		newPage := !bitmapGet(chunk.Bitmap, page)
		pageData := make([]byte, fastWritePageSize)
		if errno := o.readPageLocked(file, chunkIndex, page, pageData, remote); errno != 0 {
			o.releaseFD(key)
			return total, errno
		}
		copy(pageData[pageOff:pageOff+n], data[:n])
		written, writeErr := o.writeFile(dataFile, pageData, int64(uint64(page)*fastWritePageSize))
		if written < 0 || written > len(pageData) {
			written = max(0, min(written, len(pageData)))
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil && written != len(pageData) {
			writeErr = io.ErrShortWrite
		}
		committed := 0
		if written == len(pageData) {
			bitmapSet(chunk.Bitmap, page)
			committed = n
		} else if !newPage && written > pageOff {
			committed = min(n, written-pageOff)
		}
		if committed > 0 {
			o.markFDDirty(key)
			if off+uint64(committed) > file.length {
				file.length = off + uint64(committed)
			}
			markFastWriteMutationLocked(file)
		}
		total += committed
		o.releaseFD(key)
		if writeErr != nil {
			if errors.Is(writeErr, syscall.ENOSPC) {
				return total, syscall.ENOSPC
			}
			return total, syscall.EIO
		}
		off, data = off+uint64(n), data[n:]
	}
	return total, 0
}

func (o *fastWrite) WriteWithCount(inode Ino, remoteLength, off uint64, data []byte,
	remote fastWriteRemoteReader) (int, syscall.Errno) {
	if len(data) == 0 {
		return 0, 0
	}
	guard, errno := o.beginWrite(inode, remoteLength, off, uint64(len(data)))
	if errno != 0 {
		return 0, errno
	}
	defer guard.release()
	return o.writeLocked(guard, off, data, remote)
}

func (o *fastWrite) writeWithMutation(mutation *fastWriteMutationGuard, off uint64, data []byte,
	remote fastWriteRemoteReader) (int, syscall.Errno) {
	if len(data) == 0 {
		return 0, 0
	}
	guard, errno := o.beginWriteWithMutation(mutation, off, uint64(len(data)), false)
	if errno != 0 {
		return 0, errno
	}
	defer guard.release()
	return o.writeLocked(guard, off, data, remote)
}

func (o *fastWrite) Write(inode Ino, remoteLength, off uint64, data []byte, remote fastWriteRemoteReader) syscall.Errno {
	_, errno := o.WriteWithCount(inode, remoteLength, off, data, remote)
	return errno
}

func (o *fastWrite) Read(inode Ino, off uint64, dst []byte, remote fastWriteRemoteReader) (int, syscall.Errno) {
	file := o.get(inode)
	if file == nil {
		return remote(off, dst)
	}
	file.RLock()
	defer file.RUnlock()
	if off >= file.length {
		return 0, 0
	}
	if uint64(len(dst)) > file.length-off {
		dst = dst[:file.length-off]
	}
	read := 0
	for read < len(dst) {
		absolute := off + uint64(read)
		chunkIndex := uint32(absolute / meta.ChunkSize)
		chunkOff := absolute % meta.ChunkSize
		page := uint32(chunkOff / fastWritePageSize)
		generation, _ := fastWritePageSourceLocked(file, chunkIndex, page)
		runEnd := min(off+uint64(len(dst)), (uint64(chunkIndex)+1)*meta.ChunkSize)
		for next := page + 1; uint64(chunkIndex)*meta.ChunkSize+uint64(next)*fastWritePageSize < runEnd; next++ {
			nextGeneration, _ := fastWritePageSourceLocked(file, chunkIndex, next)
			if nextGeneration != generation {
				runEnd = uint64(chunkIndex)*meta.ChunkSize + uint64(next)*fastWritePageSize
				break
			}
		}
		n := int(runEnd - absolute)
		if generation == nil {
			nread, errno := remote(absolute, dst[read:read+n])
			if errno != 0 {
				return read, errno
			}
			clear(dst[read+nread : read+n])
		} else {
			key := fastWriteFDKey{inode, generation.ID, chunkIndex}
			dataFile, err := o.getFD(key, false)
			if err != nil {
				return read, syscall.EIO
			}
			readErr := readFullAt(dataFile, dst[read:read+n], int64(chunkOff))
			o.releaseFD(key)
			if readErr != nil {
				return read, syscall.EIO
			}
		}
		read += n
	}
	return read, 0
}

func (o *fastWrite) Length(inode Ino) (uint64, bool) {
	file := o.get(inode)
	if file == nil {
		return 0, false
	}
	file.RLock()
	length := file.length
	file.RUnlock()
	return length, true
}

func (o *fastWrite) metadataState(inode Ino) (uint64, bool, bool) {
	file := o.get(inode)
	if file == nil {
		return 0, false, false
	}
	file.RLock()
	epoch := file.metadataEpoch
	dirty := file.metadataDirty
	file.RUnlock()
	return epoch, dirty, true
}

func (o *fastWrite) metadataFlushed(inode Ino, epoch uint64) {
	if file := o.get(inode); file != nil {
		file.Lock()
		if file.metadataEpoch == epoch {
			file.metadataDirty = false
		}
		file.Unlock()
	}
}

func (o *fastWrite) finishMetadataFlush(file *fastWriteFile, epoch uint64, flushed bool) {
	if flushed && file.metadataEpoch == epoch {
		file.metadataDirty = false
	}
	file.Unlock()
}

func (o *fastWrite) zeroRangeLocked(file *fastWriteFile, off, length uint64, remote fastWriteRemoteReader) syscall.Errno {
	if length == 0 {
		return 0
	}
	generation := activeGeneration(file)
	if generation == nil {
		return syscall.EIO
	}
	end := off + length
	for off < end {
		chunkIndex := uint32(off / meta.ChunkSize)
		chunkOff := off % meta.ChunkSize
		page := uint32(chunkOff / fastWritePageSize)
		pageOff := int(chunkOff % fastWritePageSize)
		n := min(int(end-off), int(fastWritePageSize)-pageOff)
		pageData := make([]byte, fastWritePageSize)
		if pageOff != 0 || n != int(fastWritePageSize) {
			if errno := o.readPageLocked(file, chunkIndex, page, pageData, remote); errno != 0 {
				return errno
			}
		}
		clear(pageData[pageOff : pageOff+n])
		chunk := ensureFastWriteChunk(generation, chunkIndex)
		key := fastWriteFDKey{file.inode, generation.ID, chunkIndex}
		dataFile, err := o.getFD(key, true)
		if err != nil {
			return syscall.EIO
		}
		newPage := !bitmapGet(chunk.Bitmap, page)
		written, writeErr := o.writeFile(dataFile, pageData, int64(uint64(page)*fastWritePageSize))
		if writeErr == nil && written != len(pageData) {
			writeErr = io.ErrShortWrite
		}
		if written == len(pageData) {
			bitmapSet(chunk.Bitmap, page)
			o.markFDDirty(key)
			markFastWriteMutationLocked(file)
		} else if !newPage && written > 0 {
			o.markFDDirty(key)
			markFastWriteMutationLocked(file)
		}
		o.releaseFD(key)
		if writeErr != nil {
			if errors.Is(writeErr, syscall.ENOSPC) {
				return syscall.ENOSPC
			}
			return syscall.EIO
		}
		off += uint64(n)
	}
	return 0
}

func (o *fastWrite) Truncate(inode Ino, remoteLength, length uint64, remote fastWriteRemoteReader) syscall.Errno {
	mutation, errno := o.lockMutation(inode, remoteLength)
	if errno != 0 {
		return errno
	}
	defer mutation.release()
	return o.truncateWithMutation(mutation, length, remote)
}

func (o *fastWrite) truncateWithMutation(mutation *fastWriteMutationGuard, length uint64,
	remote fastWriteRemoteReader) syscall.Errno {
	file := mutation.file
	file.Lock()
	defer file.Unlock()
	if err := o.prepareMutationLocked(file); err != nil {
		return syscall.EIO
	}
	if length < file.length {
		if errno := o.zeroRangeLocked(file, length, file.length-length, remote); errno != 0 {
			return errno
		}
	}
	file.length = length
	markFastWriteMutationLocked(file)
	return 0
}

func (o *fastWrite) ZeroRange(inode Ino, remoteLength, off, length uint64, keepSize bool, remote fastWriteRemoteReader) syscall.Errno {
	mutation, errno := o.lockMutation(inode, remoteLength)
	if errno != 0 {
		return errno
	}
	defer mutation.release()
	return o.zeroRangeWithMutation(mutation, off, length, keepSize, remote)
}

func (o *fastWrite) zeroRangeWithMutation(mutation *fastWriteMutationGuard, off, length uint64,
	keepSize bool, remote fastWriteRemoteReader) syscall.Errno {
	file := mutation.file
	file.Lock()
	defer file.Unlock()
	if length > 0 {
		if err := o.prepareMutationLocked(file); err != nil {
			return syscall.EIO
		}
	}
	if errno := o.zeroRangeLocked(file, off, length, remote); errno != 0 {
		return errno
	}
	if !keepSize && off+length > file.length {
		file.length = off + length
	}
	return 0
}

func makeChunkFooter(generation *fastWriteGeneration, chunk *fastWriteChunk) []byte {
	footer := make([]byte, fastWriteFooterSize)
	copy(footer[:fastWriteBitmapSize], chunk.Bitmap)
	copy(footer[2048:2056], fastWriteFooterMagic[:])
	binary.LittleEndian.PutUint32(footer[2056:2060], fastWriteFormatVersion)
	binary.LittleEndian.PutUint64(footer[2064:2072], generation.ID)
	binary.LittleEndian.PutUint32(footer[2072:2076], chunk.Index)
	binary.LittleEndian.PutUint64(footer[2080:2088], generation.Length)
	binary.LittleEndian.PutUint64(footer[2088:2096], uint64(generation.CheckpointNS))
	binary.LittleEndian.PutUint32(footer[4092:], crc32.ChecksumIEEE(footer[:4092]))
	return footer
}

func (o *fastWrite) checkpointLocked(file *fastWriteFile) error {
	active := activeGeneration(file)
	if active == nil || !file.checkpointPending {
		return nil
	}
	active.Length = file.length
	active.CheckpointNS = time.Now().UnixNano()
	o.fdMu.Lock()
	indices := make([]int, 0, len(o.dirtyChunks))
	for key := range o.dirtyChunks {
		if key.inode == file.inode && key.gen == active.ID {
			indices = append(indices, int(key.chunk))
		}
	}
	o.fdMu.Unlock()
	if len(indices) == 0 && len(active.Chunks) != 0 {
		// A metadata-only change still needs one footer to carry the current
		// generation length across a restart.
		for index := range active.Chunks {
			indices = append(indices, int(index))
			break
		}
	}
	sort.Ints(indices)
	type syncTarget struct {
		key  fastWriteFDKey
		file *os.File
	}
	targets := make([]syncTarget, 0, len(indices))
	releaseTargets := func() {
		for _, target := range targets {
			o.releaseFD(target.key)
		}
	}
	for _, value := range indices {
		index := uint32(value)
		key := fastWriteFDKey{file.inode, active.ID, index}
		dataFile, err := o.getFD(key, true)
		if err != nil {
			releaseTargets()
			return err
		}
		targets = append(targets, syncTarget{key: key, file: dataFile})
	}
	syncTargets := func() error {
		work := make(chan syncTarget)
		var wg sync.WaitGroup
		var syncErr error
		var syncErrOnce sync.Once
		workers := min(cap(o.syncSlots), len(targets))
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for target := range work {
					if err := o.syncFile(target.file); err != nil {
						syncErrOnce.Do(func() { syncErr = err })
					}
				}
			}()
		}
		for _, target := range targets {
			work <- target
		}
		close(work)
		wg.Wait()
		return syncErr
	}
	// Persist page data before publishing the corresponding bitmap bits.
	// A single fdatasync after writing both is insufficient: a crash during
	// that fdatasync may leave a valid footer pointing at stale page data.
	if err := syncTargets(); err != nil {
		releaseTargets()
		return err
	}
	for _, target := range targets {
		index := target.key.chunk
		if err := writeFullAt(target.file, makeChunkFooter(active, active.Chunks[index]), int64(meta.ChunkSize)); err != nil {
			releaseTargets()
			return err
		}
		o.markFDDirty(target.key)
	}
	if err := syncTargets(); err != nil {
		releaseTargets()
		return err
	}
	for _, path := range []string{
		o.generationDir(file.inode, fastWriteActive, active.ID),
		o.inodeDir(file.inode),
		filepath.Join(o.root, "inodes"),
	} {
		if err := o.syncDirectory(path); err != nil {
			releaseTargets()
			return err
		}
	}
	o.fdMu.Lock()
	for _, target := range targets {
		delete(o.dirtyFD, target.key)
		delete(o.dirtyChunks, target.key)
	}
	o.fdMu.Unlock()
	releaseTargets()
	file.checkpointPending = false
	return nil
}

func (o *fastWrite) Checkpoint(inode Ino) syscall.Errno {
	mutation := o.lockExistingMutation(inode)
	if mutation == nil {
		return 0
	}
	defer mutation.release()
	file := mutation.file
	file.Lock()
	defer file.Unlock()
	if err := o.checkpointLocked(file); err != nil {
		return syscall.EIO
	}
	return 0
}

func (o *fastWrite) cloneRange(dstFD, srcFD int, srcOff, dstOff, length uint64) error {
	if o.cloneRangeHook != nil {
		return o.cloneRangeHook(dstFD, srcFD, srcOff, dstOff, length)
	}
	return cloneFileRange(dstFD, srcFD, srcOff, dstOff, length)
}

func (o *fastWrite) cloneChunkFile(sourcePath, targetPath string, generation *fastWriteGeneration,
	chunk *fastWriteChunk) (retErr error) {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := source.Close(); retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()
	info, err := source.Stat()
	if err != nil {
		return err
	}
	if info.Size() != int64(fastWriteChunkFileSize) {
		return fmt.Errorf("invalid source chunk size %d", info.Size())
	}

	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := target.Close(); retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()

	if err = o.cloneRange(int(target.Fd()), int(source.Fd()), 0, 0, fastWriteChunkFileSize); err != nil {
		if truncateErr := target.Truncate(0); truncateErr != nil {
			return truncateErr
		}
		if truncateErr := target.Truncate(int64(fastWriteChunkFileSize)); truncateErr != nil {
			return truncateErr
		}
		page := make([]byte, fastWritePageSize)
		for index := uint32(0); index < fastWritePagesPerChunk; index++ {
			if !bitmapGet(chunk.Bitmap, index) {
				continue
			}
			offset := int64(uint64(index) * fastWritePageSize)
			if readErr := readExactAt(source, page, offset); readErr != nil {
				return readErr
			}
			if !allZero(page) {
				if writeErr := writeFullAt(target, page, offset); writeErr != nil {
					return writeErr
				}
			}
		}
		if writeErr := writeFullAt(target, makeChunkFooter(generation, chunk), int64(meta.ChunkSize)); writeErr != nil {
			return writeErr
		}
	}
	if info, err = target.Stat(); err != nil {
		return err
	}
	if info.Size() != int64(fastWriteChunkFileSize) {
		return fmt.Errorf("invalid cloned chunk size %d", info.Size())
	}
	return o.syncFile(target)
}

func (o *fastWrite) cloneGenerationLocked(file *fastWriteFile, generation *fastWriteGeneration,
	targetDir string) error {
	indices := make([]int, 0, len(generation.Chunks))
	for index := range generation.Chunks {
		indices = append(indices, int(index))
	}
	sort.Ints(indices)
	if len(indices) == 0 {
		return nil
	}

	work := make(chan uint32)
	var wg sync.WaitGroup
	var cloneErr error
	var cloneErrOnce sync.Once
	workers := min(cap(o.syncSlots), len(indices))
	if workers == 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range work {
				source := filepath.Join(
					o.generationDir(file.inode, fastWriteActive, generation.ID), chunkFileName(index))
				target := filepath.Join(targetDir, chunkFileName(index))
				err := o.cloneChunkFile(source, target, generation, generation.Chunks[index])
				if err != nil {
					cloneErrOnce.Do(func() { cloneErr = err })
				}
			}
		}()
	}
	for _, value := range indices {
		work <- uint32(value)
	}
	close(work)
	wg.Wait()
	return cloneErr
}

func (o *fastWrite) rollbackClonedSeal(inodeDir, activePath, sealedPath, retiredPath string,
	retired bool, cause error) error {
	var rollbackErrors []string
	if retired {
		if err := os.Rename(retiredPath, activePath); err != nil {
			rollbackErrors = append(rollbackErrors, "restore active: "+err.Error())
		}
	}
	if err := o.removeAll(sealedPath); err != nil {
		rollbackErrors = append(rollbackErrors, "remove sealed clone: "+err.Error())
	}
	if err := o.syncDirectory(inodeDir); err != nil {
		rollbackErrors = append(rollbackErrors, "sync inode directory: "+err.Error())
	}
	if len(rollbackErrors) != 0 {
		return fmt.Errorf("%w (rollback failed: %s)", cause, strings.Join(rollbackErrors, "; "))
	}
	return cause
}

func (o *fastWrite) discardSealing(inodeDir, sealingPath string, cause error) error {
	if err := o.removeAll(sealingPath); err != nil {
		return fmt.Errorf("%w (remove incomplete seal: %v)", cause, err)
	}
	if err := o.syncDirectory(inodeDir); err != nil {
		return fmt.Errorf("%w (sync incomplete seal cleanup: %v)", cause, err)
	}
	return cause
}

func (o *fastWrite) rotateActiveLocked(file *fastWriteFile) (uint64, error) {
	active := activeGeneration(file)
	if active == nil {
		return 0, errors.New("missing active generation")
	}
	if err := o.closeGenerationFDs(file.inode, active.ID); err != nil {
		return 0, err
	}
	inodeDir := o.inodeDir(file.inode)
	activePath := o.generationDir(file.inode, fastWriteActive, active.ID)
	sealingPath := o.transientGenerationDir(file.inode, fastWriteSealing, active.ID)
	sealedPath := o.generationDir(file.inode, fastWriteSealed, active.ID)
	retiredPath := o.transientGenerationDir(file.inode, fastWriteRetired, active.ID)
	if exists, err := pathExists(sealingPath); err != nil {
		return 0, err
	} else if exists {
		if err = o.removeAll(sealingPath); err != nil {
			return 0, fmt.Errorf("remove stale Fast Write seal: %w", err)
		}
		if err = o.syncDirectory(inodeDir); err != nil {
			return 0, fmt.Errorf("sync stale Fast Write seal cleanup: %w", err)
		}
	}
	if err := os.Mkdir(sealingPath, 0700); err != nil {
		return 0, err
	}
	if err := o.cloneGenerationLocked(file, active, sealingPath); err != nil {
		return 0, o.discardSealing(inodeDir, sealingPath, err)
	}
	if err := o.syncDirectory(sealingPath); err != nil {
		return 0, o.discardSealing(inodeDir, sealingPath, err)
	}
	if err := os.Rename(sealingPath, sealedPath); err != nil {
		return 0, o.discardSealing(inodeDir, sealingPath, err)
	}
	if err := o.syncDirectory(inodeDir); err != nil {
		return 0, o.rollbackClonedSeal(inodeDir, activePath, sealedPath, retiredPath, false, err)
	}
	if err := os.Rename(activePath, retiredPath); err != nil {
		return 0, o.rollbackClonedSeal(inodeDir, activePath, sealedPath, retiredPath, false, err)
	}
	if err := o.syncDirectory(inodeDir); err != nil {
		return 0, o.rollbackClonedSeal(inodeDir, activePath, sealedPath, retiredPath, true, err)
	}
	sealed := active.ID
	next := &fastWriteGeneration{ID: file.nextGeneration, State: fastWriteActive, Chunks: make(map[uint32]*fastWriteChunk)}
	if err := os.Mkdir(o.generationDir(file.inode, fastWriteActive, next.ID), 0700); err != nil {
		return 0, o.rollbackClonedSeal(inodeDir, activePath, sealedPath, retiredPath, true,
			fmt.Errorf("create next Fast Write generation: %w", err))
	}
	active.State = fastWriteSealed
	file.nextGeneration++
	file.generations = append(file.generations, next)
	if err := o.removeAll(retiredPath); err != nil {
		logger.Warnf("clean retired Fast Write generation %d for inode %d: %s", active.ID, file.inode, err)
	}
	if err := o.syncDirectory(inodeDir); err != nil {
		return sealed, err
	}
	return sealed, nil
}

func (o *fastWrite) prepareMutationLocked(file *fastWriteFile) error {
	active := activeGeneration(file)
	if active == nil {
		return errors.New("missing active generation")
	}
	if active.CheckpointNS == 0 {
		return nil
	}
	if file.checkpointPending {
		if err := o.checkpointLocked(file); err != nil {
			return err
		}
	}
	sealed, err := o.rotateActiveLocked(file)
	if sealed != 0 {
		file.sealPending = false
	}
	return err
}

func (o *fastWrite) sealLocked(file *fastWriteFile) (uint64, error) {
	active := activeGeneration(file)
	if active == nil {
		return 0, errors.New("missing active generation")
	}
	if len(active.Chunks) == 0 && !file.sealPending {
		for i := len(file.generations) - 1; i >= 0; i-- {
			if file.generations[i].State == fastWriteSealed {
				// A prior rotation may have completed its directory changes but
				// failed the final sync. Confirm durability before retrying it.
				if err := o.syncDirectory(o.inodeDir(file.inode)); err != nil {
					return 0, err
				}
				return file.generations[i].ID, nil
			}
		}
		return 0, nil
	}
	if err := o.checkpointLocked(file); err != nil {
		return 0, err
	}
	sealed, err := o.rotateActiveLocked(file)
	if sealed != 0 {
		file.sealPending = false
	}
	return sealed, err
}

func (o *fastWrite) Seal(inode Ino) (uint64, syscall.Errno) {
	mutation := o.lockExistingMutation(inode)
	if mutation == nil {
		return 0, 0
	}
	defer mutation.release()
	file := mutation.file
	file.Lock()
	defer file.Unlock()
	id, err := o.sealLocked(file)
	if err != nil {
		return 0, syscall.EIO
	}
	return id, 0
}

func buildUploadRuns(generation *fastWriteGeneration) []fastWriteUploadRun {
	var runs []fastWriteUploadRun
	indices := make([]int, 0, len(generation.Chunks))
	for index := range generation.Chunks {
		indices = append(indices, int(index))
	}
	sort.Ints(indices)
	for _, value := range indices {
		index := uint32(value)
		for _, run := range bitmapRuns(generation.Chunks[index].Bitmap) {
			start := uint64(index)*meta.ChunkSize + uint64(run[0])*fastWritePageSize
			if start >= generation.Length {
				continue
			}
			endPage := run[1]
			if end := uint64(index)*meta.ChunkSize + uint64(endPage)*fastWritePageSize; end > generation.Length {
				endPage = uint32((generation.Length - uint64(index)*meta.ChunkSize + fastWritePageSize - 1) / fastWritePageSize)
			}
			runs = append(runs, fastWriteUploadRun{Chunk: index, StartPage: run[0], EndPage: endPage})
		}
	}
	return runs
}

func (o *fastWrite) prepareUpload(inode Ino, maxGeneration uint64) (*fastWriteUploadGeneration, syscall.Errno) {
	file := o.get(inode)
	if file == nil {
		return nil, 0
	}
	file.Lock()
	defer file.Unlock()
	active := activeGeneration(file)
	if active != nil && active.ID <= maxGeneration && len(active.Chunks) != 0 {
		if _, err := o.sealLocked(file); err != nil {
			return nil, syscall.EIO
		}
	}
	for _, generation := range file.generations {
		if generation.ID > maxGeneration {
			break
		}
		if generation.State != fastWriteSealed {
			continue
		}
		return &fastWriteUploadGeneration{
			ID: generation.ID, Length: generation.Length,
			Runs: buildUploadRuns(generation),
		}, 0
	}
	return nil, 0
}

func (o *fastWrite) readUploadRun(inode Ino, generationID uint64, run fastWriteUploadRun) ([]byte, syscall.Errno) {
	file := o.get(inode)
	if file == nil {
		return nil, syscall.ENOENT
	}
	file.RLock()
	defer file.RUnlock()
	var generation *fastWriteGeneration
	for _, candidate := range file.generations {
		if candidate.ID == generationID {
			generation = candidate
			break
		}
	}
	if generation == nil {
		return nil, syscall.ENOENT
	}
	start := uint64(run.StartPage) * fastWritePageSize
	end := uint64(run.EndPage) * fastWritePageSize
	if absoluteEnd := uint64(run.Chunk)*meta.ChunkSize + end; absoluteEnd > generation.Length {
		end -= absoluteEnd - generation.Length
	}
	data := make([]byte, end-start)
	key := fastWriteFDKey{inode, generationID, run.Chunk}
	dataFile, err := o.getFD(key, false)
	if err != nil {
		return nil, syscall.EIO
	}
	readErr := readFullAt(dataFile, data, int64(start))
	o.releaseFD(key)
	if readErr != nil {
		return nil, syscall.EIO
	}
	return data, 0
}

func (o *fastWrite) finishUpload(inode Ino, generationID uint64) syscall.Errno {
	file := o.get(inode)
	if file == nil {
		return syscall.ENOENT
	}
	file.Lock()
	defer file.Unlock()
	for index, generation := range file.generations {
		if generation.ID != generationID {
			continue
		}
		if generation.State != fastWriteSealed {
			return syscall.EINVAL
		}
		if err := o.closeGenerationFDs(inode, generationID); err != nil {
			return syscall.EIO
		}
		sealedPath := o.generationDir(inode, fastWriteSealed, generationID)
		garbagePath := o.garbageGenerationDir(inode, generationID)
		if err := os.Rename(sealedPath, garbagePath); err != nil {
			return syscall.EIO
		}
		file.generations = append(file.generations[:index], file.generations[index+1:]...)
		if err := o.syncDirectory(o.inodeDir(inode)); err != nil {
			logger.Warnf("sync Fast Write uploaded generation %d for inode %d: %s", generationID, inode, err)
		}
		if err := o.removeAll(garbagePath); err != nil {
			logger.Warnf("clean Fast Write uploaded generation %d for inode %d: %s", generationID, inode, err)
		} else if err := o.syncDirectory(o.inodeDir(inode)); err != nil {
			logger.Warnf("sync Fast Write cleanup for generation %d inode %d: %s", generationID, inode, err)
		}
		return 0
	}
	return syscall.ENOENT
}

func fastWriteFileCleanLocked(file *fastWriteFile) bool {
	if file.checkpointPending {
		return false
	}
	for _, generation := range file.generations {
		if generation.State != fastWriteActive || len(generation.Chunks) != 0 {
			return false
		}
	}
	return true
}

func (o *fastWrite) localPageSource(inode Ino, absolute uint64) (string, uint64, bool) {
	file := o.get(inode)
	if file == nil {
		return "", 0, false
	}
	file.RLock()
	defer file.RUnlock()
	chunkIndex := uint32(absolute / meta.ChunkSize)
	page := uint32((absolute % meta.ChunkSize) / fastWritePageSize)
	generation, _ := fastWritePageSourceLocked(file, chunkIndex, page)
	if generation == nil {
		return "", 0, false
	}
	return o.dataPath(inode, generation.ID, chunkIndex), uint64(page) * fastWritePageSize, true
}

func (o *fastWrite) clonePageWithMutation(mutation *fastWriteMutationGuard, dstOff uint64,
	srcPath string, srcOff uint64) (bool, syscall.Errno) {
	if mutation == nil || mutation.file == nil {
		return false, syscall.EINVAL
	}
	file := mutation.file
	file.Lock()
	defer file.Unlock()
	if err := o.prepareMutationLocked(file); err != nil {
		return false, syscall.EIO
	}
	generation := activeGeneration(file)
	if generation == nil {
		return false, syscall.EIO
	}
	chunkIndex := uint32(dstOff / meta.ChunkSize)
	page := uint32((dstOff % meta.ChunkSize) / fastWritePageSize)
	chunk := ensureFastWriteChunk(generation, chunkIndex)
	key := fastWriteFDKey{file.inode, generation.ID, chunkIndex}
	dstFile, err := o.getFD(key, true)
	if err != nil {
		return false, 0
	}
	defer o.releaseFD(key)
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return false, 0
	}
	defer srcFile.Close()
	if err = cloneFileRange(int(dstFile.Fd()), int(srcFile.Fd()), srcOff, uint64(page)*fastWritePageSize, fastWritePageSize); err != nil {
		return false, 0
	}
	bitmapSet(chunk.Bitmap, page)
	o.markFDDirty(key)
	if dstOff+fastWritePageSize > file.length {
		file.length = dstOff + fastWritePageSize
	}
	markFastWriteMutationLocked(file)
	return true, 0
}

func (o *fastWrite) CopyRange(src, dst Ino, srcRemoteLength, dstRemoteLength, offIn, offOut, size uint64,
	srcRemote, dstRemote fastWriteRemoteReader) (uint64, syscall.Errno) {
	mutation, errno := o.lockMutation(dst, dstRemoteLength)
	if errno != 0 {
		return 0, errno
	}
	defer mutation.release()
	return o.copyRangeWithMutation(mutation, src, srcRemoteLength, offIn, offOut, size, srcRemote, dstRemote)
}

func (o *fastWrite) copyRangeWithMutation(mutation *fastWriteMutationGuard, src Ino, srcRemoteLength,
	offIn, offOut, size uint64, srcRemote, dstRemote fastWriteRemoteReader) (uint64, syscall.Errno) {
	o.growRemoteLength(src, srcRemoteLength)
	var copied uint64
	buffer := make([]byte, fastWritePageSize)
	for copied < size {
		srcPos, dstPos := offIn+copied, offOut+copied
		n := min(size-copied, fastWritePageSize-srcPos%fastWritePageSize, fastWritePageSize-dstPos%fastWritePageSize)
		if n == fastWritePageSize {
			path, localOff, local := o.localPageSource(src, srcPos)
			if local {
				cloned, errno := o.clonePageWithMutation(mutation, dstPos, path, localOff)
				if errno != 0 {
					return copied, errno
				}
				if cloned {
					copied += n
					continue
				}
			}
		}
		nread, errno := o.Read(src, srcPos, buffer[:n], srcRemote)
		if errno != 0 {
			return copied, errno
		}
		clear(buffer[nread:n])
		written, errno := o.writeWithMutation(mutation, dstPos, buffer[:n], dstRemote)
		copied += uint64(written)
		if errno != 0 {
			return copied, errno
		}
	}
	return copied, 0
}

func (o *fastWrite) remove(inode Ino) error {
	file := o.get(inode)
	if file == nil {
		return nil
	}
	file.uploadMu.Lock()
	defer file.uploadMu.Unlock()
	file.mutationMu.Lock()
	defer file.mutationMu.Unlock()
	o.Lock()
	if o.files[inode] != file {
		o.Unlock()
		return nil
	}
	delete(o.files, inode)
	o.Unlock()
	file.Lock()
	defer file.Unlock()
	for _, generation := range file.generations {
		if err := o.closeGenerationFDs(inode, generation.ID); err != nil {
			return err
		}
	}
	if err := o.removeAll(o.inodeDir(inode)); err != nil {
		return err
	}
	return syncDir(filepath.Join(o.root, "inodes"))
}

func (o *fastWrite) markDeleted(inode Ino) error {
	file := o.get(inode)
	if file == nil {
		return nil
	}
	file.Lock()
	defer file.Unlock()
	file.deleted = true
	path := filepath.Join(o.inodeDir(inode), ".deleted")
	marker, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = marker.Sync(); err != nil {
		_ = marker.Close()
		return err
	}
	if err = marker.Close(); err != nil {
		return err
	}
	return syncDir(o.inodeDir(inode))
}

func (o *fastWrite) unmarkDeleted(inode Ino) error {
	file := o.get(inode)
	if file == nil {
		return nil
	}
	file.Lock()
	defer file.Unlock()
	path := filepath.Join(o.inodeDir(inode), ".deleted")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syncDir(o.inodeDir(inode)); err != nil {
		return err
	}
	file.deleted = false
	return nil
}

func (o *fastWrite) isDeleted(inode Ino) bool {
	file := o.get(inode)
	if file == nil {
		return false
	}
	file.RLock()
	deleted := file.deleted
	file.RUnlock()
	return deleted
}

func (o *fastWrite) listInodes() []Ino {
	return o.pendingInodes(false)
}

func (o *fastWrite) checkpointInodes() []Ino {
	return o.pendingInodes(true)
}

func (o *fastWrite) pendingInodes(includeDeleted bool) []Ino {
	o.RLock()
	inodes := make([]Ino, 0, len(o.files))
	for inode, file := range o.files {
		file.RLock()
		deleted := file.deleted
		clean := fastWriteFileCleanLocked(file)
		file.RUnlock()
		if (includeDeleted || !deleted) && !clean {
			inodes = append(inodes, inode)
		}
	}
	o.RUnlock()
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })
	return inodes
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}
