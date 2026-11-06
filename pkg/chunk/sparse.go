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

package chunk

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"

	"github.com/juicedata/juicefs/pkg/object"
)

const (
	SparseSliceBit        uint64 = 1 << 62
	SparsePageSize               = 4 << 10
	SparseBitmapSize             = chunkSize / SparsePageSize / 8
	sparsePayloadSize            = 4 << 20
	sparseManifestVersion        = 1
)

var sparseManifestMagic = [8]byte{'J', 'F', 'S', 'S', 'P', 'R', '0', '1'}

type SparsePageState uint8

const (
	SparsePageInherit SparsePageState = iota
	SparsePageData
	SparsePageZero
)

// SparseChunkStore is implemented by cachedStore without expanding the legacy
// ChunkStore interface used by external tests and SDK adapters.
type SparseChunkStore interface {
	UploadSparse(ctx context.Context, id uint64, logicalSize uint32, dataBitmap, zeroBitmap []byte,
		readPage func(page int, data []byte) error) (int64, error)
	ReadSparsePage(ctx context.Context, id uint64, page int, data []byte) (SparsePageState, error)
	ReadSparsePages(ctx context.Context, id uint64, firstPage int, states []SparsePageState, data []byte) error
	RemoveSparse(id uint64) error
	SetSparseStageRemover(remover func(uint64) error)
}

type sparseManifest struct {
	LogicalSize uint32
	Data        [SparseBitmapSize]byte
	Zero        [SparseBitmapSize]byte
	PartSizes   []uint32
	DataRanks   [SparseBitmapSize + 1]uint16
}

func IsSparseSliceID(id uint64) bool {
	return id&SparseSliceBit != 0
}

func sparseBit(bitmap []byte, page int) bool {
	return page >= 0 && page < chunkSize/SparsePageSize &&
		bitmap[page>>3]&(1<<uint(page&7)) != 0
}

func (m *sparseManifest) buildRanks() {
	var rank uint16
	for i, b := range m.Data {
		m.DataRanks[i] = rank
		rank += uint16(bits.OnesCount8(b))
	}
	m.DataRanks[len(m.Data)] = rank
}

func (m *sparseManifest) dataRank(page int) int {
	full := page >> 3
	rank := int(m.DataRanks[full])
	if rem := page & 7; rem != 0 {
		rank += bits.OnesCount8(m.Data[full] & byte((1<<uint(rem))-1))
	}
	return rank
}

func sparseCount(bitmap []byte) int {
	var n int
	for _, b := range bitmap {
		n += bits.OnesCount8(b)
	}
	return n
}

func (m *sparseManifest) marshal() []byte {
	n := 8 + 2 + 2 + 4 + 4 + SparseBitmapSize*2 + 4 + len(m.PartSizes)*4 + 4
	buf := make([]byte, n)
	copy(buf, sparseManifestMagic[:])
	binary.LittleEndian.PutUint16(buf[8:], sparseManifestVersion)
	binary.LittleEndian.PutUint16(buf[10:], SparsePageSize)
	binary.LittleEndian.PutUint32(buf[12:], m.LogicalSize)
	binary.LittleEndian.PutUint32(buf[16:], sparsePayloadSize)
	copy(buf[20:], m.Data[:])
	copy(buf[20+SparseBitmapSize:], m.Zero[:])
	p := 20 + SparseBitmapSize*2
	binary.LittleEndian.PutUint32(buf[p:], uint32(len(m.PartSizes)))
	p += 4
	for _, size := range m.PartSizes {
		binary.LittleEndian.PutUint32(buf[p:], size)
		p += 4
	}
	binary.LittleEndian.PutUint32(buf[p:], crc32.ChecksumIEEE(buf[:p]))
	return buf
}

func unmarshalSparseManifest(buf []byte) (*sparseManifest, error) {
	const fixed = 8 + 2 + 2 + 4 + 4 + SparseBitmapSize*2 + 4 + 4
	if len(buf) < fixed || !bytes.Equal(buf[:8], sparseManifestMagic[:]) {
		return nil, errors.New("invalid sparse manifest magic or length")
	}
	if binary.LittleEndian.Uint16(buf[8:]) != sparseManifestVersion ||
		binary.LittleEndian.Uint16(buf[10:]) != SparsePageSize ||
		binary.LittleEndian.Uint32(buf[16:]) != sparsePayloadSize {
		return nil, errors.New("unsupported sparse manifest format")
	}
	logical := binary.LittleEndian.Uint32(buf[12:])
	if logical == 0 || logical > chunkSize {
		return nil, fmt.Errorf("invalid sparse logical size %d", logical)
	}
	p := 20 + SparseBitmapSize*2
	parts := int(binary.LittleEndian.Uint32(buf[p:]))
	p += 4
	if parts < 0 || len(buf) != p+parts*4+4 {
		return nil, errors.New("invalid sparse manifest part table")
	}
	if crc32.ChecksumIEEE(buf[:len(buf)-4]) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, errors.New("sparse manifest checksum mismatch")
	}
	m := &sparseManifest{LogicalSize: logical, PartSizes: make([]uint32, parts)}
	copy(m.Data[:], buf[20:20+SparseBitmapSize])
	copy(m.Zero[:], buf[20+SparseBitmapSize:20+SparseBitmapSize*2])
	for i := range m.Data {
		if m.Data[i]&m.Zero[i] != 0 {
			return nil, errors.New("overlapping sparse data and zero bitmaps")
		}
	}
	for i := range m.PartSizes {
		m.PartSizes[i] = binary.LittleEndian.Uint32(buf[p:])
		p += 4
		if m.PartSizes[i] == 0 || m.PartSizes[i] > sparsePayloadSize ||
			m.PartSizes[i]%SparsePageSize != 0 {
			return nil, errors.New("invalid sparse payload part size")
		}
	}
	expectedParts := (sparseCount(m.Data[:])*SparsePageSize + sparsePayloadSize - 1) / sparsePayloadSize
	if expectedParts != len(m.PartSizes) {
		return nil, errors.New("sparse manifest payload count mismatch")
	}
	m.buildRanks()
	return m, nil
}

func (store *cachedStore) sparsePrefix(id uint64) string {
	if store.conf.HashPrefix {
		return fmt.Sprintf("chunks/%02X/%v/%v_sparse", id%256, id/1000/1000, id)
	}
	return fmt.Sprintf("chunks/%v/%v/%v_sparse", id/1000/1000, id/1000, id)
}

func (store *cachedStore) sparseManifestKey(id uint64) string {
	return store.sparsePrefix(id) + ".manifest"
}

func (store *cachedStore) sparsePartKey(id uint64, part int, size uint32) string {
	return fmt.Sprintf("%s_%d_%d", store.sparsePrefix(id), part, size)
}

func (store *cachedStore) UploadSparse(ctx context.Context, id uint64, logicalSize uint32,
	dataBitmap, zeroBitmap []byte, readPage func(page int, data []byte) error) (int64, error) {
	if !IsSparseSliceID(id) || len(dataBitmap) != SparseBitmapSize || len(zeroBitmap) != SparseBitmapSize {
		return 0, errors.New("invalid sparse upload")
	}
	m := &sparseManifest{LogicalSize: logicalSize}
	copy(m.Data[:], dataBitmap)
	copy(m.Zero[:], zeroBitmap)
	for i := range m.Data {
		if m.Data[i]&m.Zero[i] != 0 {
			return 0, errors.New("overlapping sparse data and zero bitmaps")
		}
	}
	m.buildRanks()

	part := make([]byte, 0, sparsePayloadSize)
	page := make([]byte, SparsePageSize)
	var uploaded int64
	partIndex := 0
	putPart := func() error {
		if len(part) == 0 {
			return nil
		}
		size := uint32(len(part))
		if err := store.storage.Put(ctx, store.sparsePartKey(id, partIndex, size), bytes.NewReader(part)); err != nil {
			return err
		}
		m.PartSizes = append(m.PartSizes, size)
		uploaded += int64(size)
		partIndex++
		part = make([]byte, 0, sparsePayloadSize)
		return nil
	}
	for i := 0; i < chunkSize/SparsePageSize; i++ {
		if !sparseBit(m.Data[:], i) {
			continue
		}
		if err := readPage(i, page); err != nil {
			return uploaded, err
		}
		part = append(part, page...)
		if len(part) == sparsePayloadSize {
			if err := putPart(); err != nil {
				return uploaded, err
			}
		}
	}
	if err := putPart(); err != nil {
		return uploaded, err
	}
	manifest := m.marshal()
	if err := store.storage.Put(ctx, store.sparseManifestKey(id), bytes.NewReader(manifest)); err != nil {
		return uploaded, err
	}
	uploaded += int64(len(manifest))
	store.sparseManifestMu.Lock()
	store.sparseManifests[id] = m
	store.sparseManifestMu.Unlock()
	return uploaded, nil
}

func (store *cachedStore) loadSparseManifest(ctx context.Context, id uint64) (*sparseManifest, error) {
	store.sparseManifestMu.Lock()
	m := store.sparseManifests[id]
	store.sparseManifestMu.Unlock()
	if m != nil {
		return m, nil
	}
	r, err := store.storage.Get(ctx, store.sparseManifestKey(id), 0, -1)
	if err != nil {
		return nil, fmt.Errorf("load sparse manifest %d: %w", id, err)
	}
	buf, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		return nil, fmt.Errorf("read sparse manifest %d: %w", id, err)
	}
	m, err = unmarshalSparseManifest(buf)
	if err != nil {
		return nil, fmt.Errorf("decode sparse manifest %d: %w", id, err)
	}
	store.sparseManifestMu.Lock()
	if old := store.sparseManifests[id]; old != nil {
		m = old
	} else {
		store.sparseManifests[id] = m
	}
	store.sparseManifestMu.Unlock()
	return m, nil
}

func (store *cachedStore) ReadSparsePage(ctx context.Context, id uint64, page int, data []byte) (SparsePageState, error) {
	if !IsSparseSliceID(id) || page < 0 || page >= chunkSize/SparsePageSize || len(data) != SparsePageSize {
		return SparsePageInherit, errors.New("invalid sparse page read")
	}
	m, err := store.loadSparseManifest(ctx, id)
	if err != nil {
		return SparsePageInherit, err
	}
	if sparseBit(m.Zero[:], page) {
		clear(data)
		return SparsePageZero, nil
	}
	if !sparseBit(m.Data[:], page) {
		return SparsePageInherit, nil
	}
	rank := m.dataRank(page)
	part := rank * SparsePageSize / sparsePayloadSize
	off := rank * SparsePageSize % sparsePayloadSize
	if part >= len(m.PartSizes) || off+SparsePageSize > int(m.PartSizes[part]) {
		return SparsePageInherit, errors.New("sparse page offset outside payload")
	}
	r, err := store.storage.Get(ctx, store.sparsePartKey(id, part, m.PartSizes[part]), int64(off), SparsePageSize)
	if err != nil {
		return SparsePageInherit, fmt.Errorf("load sparse payload %d page %d: %w", id, page, err)
	}
	_, err = io.ReadFull(r, data)
	_ = r.Close()
	if err != nil {
		return SparsePageInherit, fmt.Errorf("read sparse payload %d page %d: %w", id, page, err)
	}
	return SparsePageData, nil
}

func (store *cachedStore) ReadSparsePages(ctx context.Context, id uint64, firstPage int, states []SparsePageState, data []byte) error {
	if !IsSparseSliceID(id) || firstPage < 0 || firstPage+len(states) > chunkSize/SparsePageSize ||
		len(data) != len(states)*SparsePageSize {
		return errors.New("invalid sparse page range read")
	}
	m, err := store.loadSparseManifest(ctx, id)
	if err != nil {
		return err
	}
	for i := range states {
		page := firstPage + i
		switch {
		case sparseBit(m.Zero[:], page):
			states[i] = SparsePageZero
			clear(data[i*SparsePageSize : (i+1)*SparsePageSize])
		case sparseBit(m.Data[:], page):
			states[i] = SparsePageData
		default:
			states[i] = SparsePageInherit
		}
	}

	for i := 0; i < len(states); {
		if states[i] != SparsePageData {
			i++
			continue
		}
		rank := m.dataRank(firstPage + i)
		part := rank * SparsePageSize / sparsePayloadSize
		partOffset := rank * SparsePageSize % sparsePayloadSize
		if part >= len(m.PartSizes) {
			return errors.New("sparse page range outside payload")
		}
		j := i + 1
		for j < len(states) {
			if states[j] != SparsePageData {
				j++
				continue
			}
			nextRank := m.dataRank(firstPage + j)
			if nextRank*SparsePageSize/sparsePayloadSize != part {
				break
			}
			j++
		}
		dataPages := 0
		for k := i; k < j; k++ {
			if states[k] == SparsePageData {
				dataPages++
			}
		}
		size := dataPages * SparsePageSize
		if partOffset+size > int(m.PartSizes[part]) {
			return errors.New("sparse page range outside payload part")
		}
		r, err := store.storage.Get(ctx, store.sparsePartKey(id, part, m.PartSizes[part]), int64(partOffset), int64(size))
		if err != nil {
			return fmt.Errorf("load sparse payload %d pages %d-%d: %w", id, firstPage+i, firstPage+j-1, err)
		}
		var readErr error
		if dataPages == j-i {
			_, readErr = io.ReadFull(r, data[i*SparsePageSize:j*SparsePageSize])
		} else {
			packed := make([]byte, size)
			_, readErr = io.ReadFull(r, packed)
			off := 0
			for k := i; k < j; k++ {
				if states[k] == SparsePageData {
					copy(data[k*SparsePageSize:(k+1)*SparsePageSize], packed[off:off+SparsePageSize])
					off += SparsePageSize
				}
			}
		}
		_ = r.Close()
		if readErr != nil {
			return fmt.Errorf("read sparse payload %d pages %d-%d: %w", id, firstPage+i, firstPage+j-1, readErr)
		}
		i = j
	}
	return nil
}

func (store *cachedStore) RemoveSparse(id uint64) error {
	store.sparseStageMu.Lock()
	remover := store.sparseStageRemover
	store.sparseStageMu.Unlock()
	if remover != nil {
		if err := remover(id); err != nil {
			return err
		}
	}

	ctx := context.Background()
	objects, err := object.ListAll(ctx, store.storage, store.sparsePrefix(id), "", false, false)
	if err != nil {
		return err
	}
	for obj := range objects {
		if obj == nil {
			return errors.New("list sparse objects failed")
		}
		if obj.IsDir() {
			continue
		}
		if err := store.storage.Delete(ctx, obj.Key()); err != nil {
			return err
		}
	}
	store.sparseManifestMu.Lock()
	delete(store.sparseManifests, id)
	store.sparseManifestMu.Unlock()
	return nil
}

func (store *cachedStore) SetSparseStageRemover(remover func(uint64) error) {
	store.sparseStageMu.Lock()
	store.sparseStageRemover = remover
	store.sparseStageMu.Unlock()
}

var _ SparseChunkStore = (*cachedStore)(nil)
