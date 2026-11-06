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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	sparseStageHeaderVersion = 1
	sparseStageSlotSize      = 8 << 10
	sparseStageDataOffset    = 2 * sparseStageSlotSize
)

var sparseStageMagic = [8]byte{'J', 'F', 'S', 'S', 'T', 'G', '0', '1'}

type sparseStageKey struct {
	Inode meta.Ino
	Chunk uint32
}

type sparseGeneration struct {
	Version     uint16 `json:"version"`
	Inode       meta.Ino
	Chunk       uint32
	ID          uint64
	LogicalSize uint32
	BaseEOF     uint64
	RealEOF     uint64
	Mtime       int64
	DataBitmap  []byte
	ZeroBitmap  []byte
	DataPath    string
	Frozen      bool
	Sequence    uint64
}

type sparseStaging struct {
	sync.RWMutex
	publishMu sync.Mutex

	conf   *Config
	meta   meta.Meta
	store  chunk.SparseChunkStore
	reader *dataReader
	root   string

	active   map[sparseStageKey]*sparseGeneration
	pending  map[sparseStageKey][]*sparseGeneration
	byID     map[uint64]*sparseGeneration
	localIDs sync.Map

	watermarkPublishing atomic.Bool
}

func newSparseStaging(conf *Config, m meta.Meta, store chunk.ChunkStore, reader DataReader) (*sparseStaging, error) {
	if conf.Chunk == nil || !conf.Chunk.SparseChunkSlice {
		return nil, nil
	}
	if !conf.Chunk.Writeback {
		return nil, errors.New("sparse-chunk-slice requires writeback")
	}
	sparseStore, ok := store.(chunk.SparseChunkStore)
	if !ok {
		return nil, errors.New("chunk store does not support sparse manifests")
	}
	dr, ok := reader.(*dataReader)
	if !ok {
		return nil, errors.New("data reader does not support sparse staging")
	}
	dirs := utils.SplitDir(conf.Chunk.CacheDir)
	if len(dirs) == 0 || conf.Chunk.CacheDir == "memory" {
		return nil, errors.New("sparse-chunk-slice requires a disk cache directory")
	}
	matches, _ := filepath.Glob(dirs[0])
	if len(matches) > 0 {
		dirs[0] = matches[0]
	}
	root := filepath.Join(strings.TrimSpace(dirs[0]), "sparse-staging")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	s := &sparseStaging{
		conf: conf, meta: m, store: sparseStore, reader: dr, root: root,
		active:  make(map[sparseStageKey]*sparseGeneration),
		pending: make(map[sparseStageKey][]*sparseGeneration),
		byID:    make(map[uint64]*sparseGeneration),
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	sparseStore.SetSparseStageRemover(s.removeByID)
	dr.sparse = s
	return s, nil
}

func (s *sparseStaging) path(inode meta.Ino, chunkIndex uint32, id uint64) string {
	base := fmt.Sprintf("%d-%d-%d", inode, chunkIndex, id)
	return filepath.Join(s.root, base+".stage")
}

func (s *sparseStaging) newGenerationLocked(key sparseStageKey, baseEOF, realEOF uint64) (*sparseGeneration, error) {
	var id uint64
	if st := s.meta.NewSlice(meta.Background(), &id); st != 0 {
		return nil, st
	}
	if id&chunk.SparseSliceBit != 0 {
		return nil, errors.New("slice ID space reached sparse discriminator")
	}
	id |= chunk.SparseSliceBit
	dataPath := s.path(key.Inode, key.Chunk, id)
	f, err := os.OpenFile(dataPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(sparseStageDataOffset + meta.ChunkSize)
	_ = f.Close()
	if err != nil {
		_ = os.Remove(dataPath)
		return nil, err
	}
	g := &sparseGeneration{
		Version: 1, Inode: key.Inode, Chunk: key.Chunk, ID: id,
		LogicalSize: meta.ChunkSize, BaseEOF: baseEOF, RealEOF: realEOF, Mtime: time.Now().UnixNano(),
		DataBitmap: make([]byte, chunk.SparseBitmapSize),
		ZeroBitmap: make([]byte, chunk.SparseBitmapSize),
		DataPath:   dataPath,
	}
	s.active[key] = g
	s.byID[g.ID] = g
	s.localIDs.Store(g.ID, struct{}{})
	return g, nil
}

func bitmapSet(bitmap []byte, page int, value bool) {
	mask := byte(1 << uint(page&7))
	if value {
		bitmap[page>>3] |= mask
	} else {
		bitmap[page>>3] &^= mask
	}
}

func bitmapGet(bitmap []byte, page int) bool {
	return bitmap[page>>3]&(1<<uint(page&7)) != 0
}

func (s *sparseStaging) readGlobalPageLocked(key sparseStageKey, pageIndex int, data []byte) error {
	clear(data)
	var slices []meta.Slice
	if st := s.meta.Read(meta.Background(), key.Inode, key.Chunk, &slices); st != 0 {
		return st
	}
	p := chunk.NewPage(data)
	var n int
	sparse := false
	for _, slice := range slices {
		if meta.IsSparseSliceID(slice.Id) {
			sparse = true
			break
		}
	}
	if sparse {
		overlayReader, ok := s.meta.(meta.OverlayReader)
		if !ok {
			p.Release()
			return syscall.EIO
		}
		var overlays []meta.OverlaySlice
		if st := overlayReader.ReadOverlay(meta.Background(), key.Inode, key.Chunk, &overlays); st != 0 {
			p.Release()
			return st
		}
		n = s.reader.readOverlaySlices(meta.Background(), p, overlays, uint32(pageIndex*chunk.SparsePageSize))
	} else {
		n = s.reader.Read(meta.Background(), p, slices, uint32(pageIndex*chunk.SparsePageSize))
	}
	p.Release()
	if n != len(data) {
		return syscall.EIO
	}
	return nil
}

func (s *sparseStaging) applyGenerationPageLocked(g *sparseGeneration, pageIndex int, data []byte) error {
	if bitmapGet(g.ZeroBitmap, pageIndex) {
		clear(data)
		return nil
	}
	if !bitmapGet(g.DataBitmap, pageIndex) {
		return nil
	}
	f, err := os.Open(g.DataPath)
	if err != nil {
		return err
	}
	_, err = f.ReadAt(data, int64(sparseStageDataOffset+pageIndex*chunk.SparsePageSize))
	_ = f.Close()
	return err
}

func (s *sparseStaging) readBasePageLocked(key sparseStageKey, current *sparseGeneration, pageIndex int, data []byte) error {
	if err := s.readGlobalPageLocked(key, pageIndex, data); err != nil {
		return err
	}
	gens := append([]*sparseGeneration(nil), s.pending[key]...)
	sort.Slice(gens, func(i, j int) bool { return gens[i].ID < gens[j].ID })
	for _, g := range gens {
		if g != current {
			if err := s.applyGenerationPageLocked(g, pageIndex, data); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sparseStaging) Write(inode meta.Ino, off uint64, data []byte, initialLength uint64) error {
	s.Lock()
	defer s.Unlock()
	for len(data) > 0 {
		key := sparseStageKey{Inode: inode, Chunk: uint32(off / meta.ChunkSize)}
		chunkOff := uint32(off % meta.ChunkSize)
		n := min(len(data), int(meta.ChunkSize-chunkOff))
		g := s.active[key]
		if g == nil {
			var err error
			g, err = s.newGenerationLocked(key, initialLength, max(initialLength, off+uint64(n)))
			if err != nil {
				return err
			}
		}
		f, err := os.OpenFile(g.DataPath, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		written := 0
		for written < n {
			pos := int(chunkOff) + written
			pageIndex := pos / chunk.SparsePageSize
			inPage := pos % chunk.SparsePageSize
			take := min(n-written, chunk.SparsePageSize-inPage)
			var page []byte
			if inPage == 0 && take == chunk.SparsePageSize {
				page = data[written : written+take]
			} else {
				page = make([]byte, chunk.SparsePageSize)
				if bitmapGet(g.DataBitmap, pageIndex) {
					if _, err = f.ReadAt(page, int64(sparseStageDataOffset+pageIndex*chunk.SparsePageSize)); err != nil {
						_ = f.Close()
						return err
					}
				} else if !bitmapGet(g.ZeroBitmap, pageIndex) {
					if err = s.readBasePageLocked(key, g, pageIndex, page); err != nil {
						_ = f.Close()
						return err
					}
				}
				copy(page[inPage:inPage+take], data[written:written+take])
			}
			allZero := true
			for _, b := range page {
				if b != 0 {
					allZero = false
					break
				}
			}
			if allZero {
				bitmapSet(g.DataBitmap, pageIndex, false)
				bitmapSet(g.ZeroBitmap, pageIndex, true)
			} else {
				if _, err = f.WriteAt(page, int64(sparseStageDataOffset+pageIndex*chunk.SparsePageSize)); err != nil {
					_ = f.Close()
					return err
				}
				bitmapSet(g.DataBitmap, pageIndex, true)
				bitmapSet(g.ZeroBitmap, pageIndex, false)
			}
			written += take
		}
		_ = f.Close()
		g.RealEOF = max(g.RealEOF, off+uint64(n))
		g.Mtime = time.Now().UnixNano()
		off += uint64(n)
		data = data[n:]
	}
	s.maybeTriggerWatermarkLocked()
	return nil
}

func (s *sparseStaging) overlay(inode meta.Ino, chunkIndex uint32, off uint32, data []byte) error {
	s.RLock()
	defer s.RUnlock()
	return s.overlayLocked(inode, chunkIndex, off, data)
}

func (s *sparseStaging) overlayLocked(inode meta.Ino, chunkIndex uint32, off uint32, data []byte) error {
	key := sparseStageKey{Inode: inode, Chunk: chunkIndex}
	gens := append([]*sparseGeneration(nil), s.pending[key]...)
	if g := s.active[key]; g != nil {
		gens = append(gens, g)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i].ID < gens[j].ID })
	for _, g := range gens {
		start := int(off)
		end := start + len(data)
		if start >= int(g.LogicalSize) {
			continue
		}
		end = min(end, int(g.LogicalSize))
		firstPage := start / chunk.SparsePageSize
		lastPage := (end - 1) / chunk.SparsePageSize
		page := make([]byte, chunk.SparsePageSize)
		for pageIndex := firstPage; pageIndex <= lastPage; pageIndex++ {
			if !bitmapGet(g.DataBitmap, pageIndex) && !bitmapGet(g.ZeroBitmap, pageIndex) {
				continue
			}
			if err := s.applyGenerationPageLocked(g, pageIndex, page); err != nil {
				return err
			}
			pageStart := pageIndex * chunk.SparsePageSize
			copyStart := max(start, pageStart)
			copyEnd := min(end, pageStart+chunk.SparsePageSize)
			copy(data[copyStart-start:copyEnd-start], page[copyStart-pageStart:copyEnd-pageStart])
		}
	}
	return nil
}

func (s *sparseStaging) beginRead(inode meta.Ino, chunkIndex uint32) bool {
	s.RLock()
	key := sparseStageKey{Inode: inode, Chunk: chunkIndex}
	if s.active[key] != nil || len(s.pending[key]) > 0 {
		return true
	}
	s.RUnlock()
	return false
}

func (s *sparseStaging) endRead() {
	s.RUnlock()
}

func (s *sparseStaging) Length(inode meta.Ino) uint64 {
	s.RLock()
	defer s.RUnlock()
	var length uint64
	for key, gens := range s.pending {
		if key.Inode == inode {
			for _, g := range gens {
				length = max(length, g.RealEOF)
			}
		}
	}
	for key, g := range s.active {
		if key.Inode == inode {
			length = max(length, g.RealEOF)
		}
	}
	return length
}

func (s *sparseStaging) hasGeneration(id uint64) bool {
	_, ok := s.localIDs.Load(id)
	return ok
}

func (s *sparseStaging) removeGenerationLocked(g *sparseGeneration) {
	key := sparseStageKey{Inode: g.Inode, Chunk: g.Chunk}
	if s.active[key] == g {
		delete(s.active, key)
	}
	if list := s.pending[key]; len(list) > 0 {
		for i, candidate := range list {
			if candidate == g {
				list = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(s.pending, key)
		} else {
			s.pending[key] = list
		}
	}
	delete(s.byID, g.ID)
	s.localIDs.Delete(g.ID)
}

func (s *sparseStaging) removeByID(id uint64) error {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	s.Lock()
	g := s.byID[id]
	if g == nil {
		s.Unlock()
		return nil
	}
	if err := os.Remove(g.DataPath); err != nil && !os.IsNotExist(err) {
		s.Unlock()
		return fmt.Errorf("remove sparse staging %s: %w", g.DataPath, err)
	}
	s.removeGenerationLocked(g)
	s.Unlock()
	return nil
}

func marshalSparseStageHeader(g *sparseGeneration, sequence uint64) []byte {
	buf := make([]byte, sparseStageSlotSize)
	copy(buf, sparseStageMagic[:])
	binary.LittleEndian.PutUint16(buf[8:], sparseStageHeaderVersion)
	if g.Frozen {
		binary.LittleEndian.PutUint16(buf[10:], 1)
	}
	binary.LittleEndian.PutUint64(buf[16:], sequence)
	binary.LittleEndian.PutUint64(buf[24:], uint64(g.Inode))
	binary.LittleEndian.PutUint32(buf[32:], g.Chunk)
	binary.LittleEndian.PutUint32(buf[36:], g.LogicalSize)
	binary.LittleEndian.PutUint64(buf[40:], g.ID)
	binary.LittleEndian.PutUint64(buf[48:], g.BaseEOF)
	binary.LittleEndian.PutUint64(buf[56:], g.RealEOF)
	binary.LittleEndian.PutUint64(buf[64:], uint64(g.Mtime))
	copy(buf[72:], g.DataBitmap)
	copy(buf[72+chunk.SparseBitmapSize:], g.ZeroBitmap)
	binary.LittleEndian.PutUint32(buf[sparseStageSlotSize-4:], crc32.ChecksumIEEE(buf[:sparseStageSlotSize-4]))
	return buf
}

func unmarshalSparseStageHeader(buf []byte) (*sparseGeneration, error) {
	if len(buf) != sparseStageSlotSize || string(buf[:8]) != string(sparseStageMagic[:]) ||
		binary.LittleEndian.Uint16(buf[8:]) != sparseStageHeaderVersion {
		return nil, errors.New("invalid sparse stage header")
	}
	expected := binary.LittleEndian.Uint32(buf[sparseStageSlotSize-4:])
	if crc32.ChecksumIEEE(buf[:sparseStageSlotSize-4]) != expected {
		return nil, errors.New("invalid sparse stage header checksum")
	}
	g := &sparseGeneration{
		Version:     1,
		Frozen:      binary.LittleEndian.Uint16(buf[10:])&1 != 0,
		Sequence:    binary.LittleEndian.Uint64(buf[16:]),
		Inode:       meta.Ino(binary.LittleEndian.Uint64(buf[24:])),
		Chunk:       binary.LittleEndian.Uint32(buf[32:]),
		LogicalSize: binary.LittleEndian.Uint32(buf[36:]),
		ID:          binary.LittleEndian.Uint64(buf[40:]),
		BaseEOF:     binary.LittleEndian.Uint64(buf[48:]),
		RealEOF:     binary.LittleEndian.Uint64(buf[56:]),
		Mtime:       int64(binary.LittleEndian.Uint64(buf[64:])),
		DataBitmap:  append([]byte(nil), buf[72:72+chunk.SparseBitmapSize]...),
		ZeroBitmap:  append([]byte(nil), buf[72+chunk.SparseBitmapSize:72+2*chunk.SparseBitmapSize]...),
	}
	if g.Sequence == 0 || !meta.IsSparseSliceID(g.ID) || g.LogicalSize == 0 || g.LogicalSize > meta.ChunkSize {
		return nil, errors.New("invalid sparse stage generation")
	}
	return g, nil
}

func (s *sparseStaging) syncGeneration(g *sparseGeneration) error {
	f, err := os.OpenFile(g.DataPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	sequence := g.Sequence + 1
	header := marshalSparseStageHeader(g, sequence)
	slot := sequence % 2
	if _, err = f.WriteAt(header, int64(slot*sparseStageSlotSize)); err == nil {
		err = f.Sync()
	}
	_ = f.Close()
	if err != nil {
		return err
	}
	g.Sequence = sequence
	dir, err := os.Open(s.root)
	if err != nil {
		return err
	}
	err = dir.Sync()
	_ = dir.Close()
	return err
}

func (s *sparseStaging) registerGeneration(g *sparseGeneration) error {
	value := meta.Slice{Id: g.ID, Size: meta.ChunkSize, Len: meta.ChunkSize}
	if st := s.meta.WriteEOF(meta.Background(), g.Inode, g.Chunk, 0, value, time.Unix(0, g.Mtime), g.RealEOF); st != 0 {
		return st
	}
	return nil
}

func (s *sparseStaging) syncAndRegisterGeneration(g *sparseGeneration) error {
	if err := s.syncGeneration(g); err != nil {
		return err
	}
	return s.registerGeneration(g)
}

func sortSparseGenerations(gens []*sparseGeneration) {
	sort.Slice(gens, func(i, j int) bool {
		if gens[i].Inode != gens[j].Inode {
			return gens[i].Inode < gens[j].Inode
		}
		if gens[i].Mtime != gens[j].Mtime {
			return gens[i].Mtime < gens[j].Mtime
		}
		return gens[i].ID < gens[j].ID
	})
}

func (s *sparseStaging) SyncInode(inode meta.Ino) error {
	s.Lock()
	defer s.Unlock()
	var gens []*sparseGeneration
	for key, g := range s.active {
		if key.Inode == inode {
			gens = append(gens, g)
		}
	}
	for key, pending := range s.pending {
		if key.Inode == inode {
			gens = append(gens, pending...)
		}
	}
	sortSparseGenerations(gens)
	for _, g := range gens {
		if err := s.syncAndRegisterGeneration(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *sparseStaging) SyncAll() error {
	s.Lock()
	defer s.Unlock()
	gens := make([]*sparseGeneration, 0, len(s.byID))
	for _, g := range s.active {
		gens = append(gens, g)
	}
	for _, pending := range s.pending {
		gens = append(gens, pending...)
	}
	sortSparseGenerations(gens)
	for _, g := range gens {
		if err := s.syncAndRegisterGeneration(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *sparseStaging) freezeLocked(filter map[meta.Ino]struct{}) error {
	var gens []*sparseGeneration
	for key, g := range s.active {
		if filter != nil {
			if _, ok := filter[key.Inode]; !ok {
				continue
			}
		}
		gens = append(gens, g)
	}
	sortSparseGenerations(gens)
	for _, g := range gens {
		key := sparseStageKey{Inode: g.Inode, Chunk: g.Chunk}
		g.Frozen = true
		if err := s.syncAndRegisterGeneration(g); err != nil {
			return err
		}
		s.pending[key] = append(s.pending[key], g)
		delete(s.active, key)
	}
	return nil
}

func (s *sparseStaging) publish(filter map[meta.Ino]struct{}) (int, error) {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	s.Lock()
	if err := s.freezeLocked(filter); err != nil {
		s.Unlock()
		return 0, err
	}
	var gens []*sparseGeneration
	for key, pending := range s.pending {
		if filter != nil {
			if _, ok := filter[key.Inode]; !ok {
				continue
			}
		}
		gens = append(gens, pending...)
	}
	s.Unlock()
	sort.Slice(gens, func(i, j int) bool { return gens[i].ID < gens[j].ID })

	for _, g := range gens {
		f, err := os.Open(g.DataPath)
		if err != nil {
			return 0, err
		}
		readPage := func(page int, data []byte) error {
			_, err := f.ReadAt(data, int64(sparseStageDataOffset+page*chunk.SparsePageSize))
			return err
		}
		physicalSize, err := s.store.UploadSparse(meta.Background(), g.ID, g.LogicalSize,
			g.DataBitmap, g.ZeroBitmap, readPage)
		_ = f.Close()
		if err != nil {
			return 0, err
		}
		if physicalSize <= 0 || physicalSize > int64(^uint32(0)) {
			return 0, fmt.Errorf("invalid sparse physical size %d", physicalSize)
		}
	}

	published := 0
	for _, g := range gens {
		s.reader.Invalidate(g.Inode, uint64(g.Chunk)*meta.ChunkSize, uint64(g.LogicalSize))
		s.Lock()
		key := sparseStageKey{Inode: g.Inode, Chunk: g.Chunk}
		list := s.pending[key]
		for i, candidate := range list {
			if candidate == g {
				s.pending[key] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(s.pending[key]) == 0 {
			delete(s.pending, key)
		}
		delete(s.byID, g.ID)
		s.localIDs.Delete(g.ID)
		s.Unlock()
		_ = os.Remove(g.DataPath)
		published++
	}
	return published, nil
}

func (s *sparseStaging) PublishAll() (int, error) {
	return s.publish(nil)
}

func (s *sparseStaging) PublishInodes(inodes []meta.Ino) (int, error) {
	filter := make(map[meta.Ino]struct{}, len(inodes))
	for _, inode := range inodes {
		filter[inode] = struct{}{}
	}
	return s.publish(filter)
}

func (s *sparseStaging) stagedBytesLocked() uint64 {
	var pages uint64
	count := func(g *sparseGeneration) {
		for i := range g.DataBitmap {
			for b := g.DataBitmap[i]; b != 0; b &= b - 1 {
				pages++
			}
		}
	}
	for _, g := range s.active {
		count(g)
	}
	for _, gens := range s.pending {
		for _, g := range gens {
			count(g)
		}
	}
	return pages * chunk.SparsePageSize
}

func (s *sparseStaging) maybeTriggerWatermarkLocked() {
	if s.conf.Chunk.StageUsageHigh <= 0 || s.conf.Chunk.CacheSize == 0 {
		return
	}
	high := uint64(float64(s.conf.Chunk.CacheSize) * float64(s.conf.Chunk.StageUsageHigh))
	if s.stagedBytesLocked() < high || !s.watermarkPublishing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.watermarkPublishing.Store(false)
		if _, err := s.PublishAll(); err != nil {
			logger.Errorf("publish sparse staging at high watermark: %s", err)
		}
	}()
}

func (s *sparseStaging) recover() error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".stage") {
			continue
		}
		dataPath := filepath.Join(s.root, entry.Name())
		f, err := os.Open(dataPath)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil || info.Size() != sparseStageDataOffset+meta.ChunkSize {
			_ = f.Close()
			return fmt.Errorf("invalid sparse stage %s", dataPath)
		}
		var recovered *sparseGeneration
		for slot := 0; slot < 2; slot++ {
			buf := make([]byte, sparseStageSlotSize)
			if _, err = f.ReadAt(buf, int64(slot*sparseStageSlotSize)); err != nil {
				continue
			}
			candidate, decodeErr := unmarshalSparseStageHeader(buf)
			if decodeErr == nil && (recovered == nil || candidate.Sequence > recovered.Sequence) {
				recovered = candidate
			}
		}
		_ = f.Close()
		if recovered == nil {
			_ = os.Remove(dataPath)
			continue
		}
		g := recovered
		g.DataPath = dataPath
		key := sparseStageKey{Inode: g.Inode, Chunk: g.Chunk}
		s.byID[g.ID] = g
		s.localIDs.Store(g.ID, struct{}{})
		if g.Frozen {
			s.pending[key] = append(s.pending[key], g)
		} else if old := s.active[key]; old == nil || old.ID < g.ID {
			if old != nil {
				old.Frozen = true
				s.pending[key] = append(s.pending[key], old)
			}
			s.active[key] = g
		} else {
			g.Frozen = true
			s.pending[key] = append(s.pending[key], g)
		}
	}
	for key := range s.pending {
		sort.Slice(s.pending[key], func(i, j int) bool { return s.pending[key][i].ID < s.pending[key][j].ID })
	}
	gens := make([]*sparseGeneration, 0, len(s.byID))
	for _, g := range s.byID {
		gens = append(gens, g)
	}
	sortSparseGenerations(gens)
	for _, g := range gens {
		if err := s.registerGeneration(g); err != nil {
			if errors.Is(err, syscall.ENOENT) {
				s.removeGenerationLocked(g)
				_ = os.Remove(g.DataPath)
				continue
			}
			return err
		}
	}
	return nil
}
