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

package meta

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta/pb"
	"github.com/stretchr/testify/require"
)

func TestSparseSlicePositiveIDAndEOF(t *testing.T) {
	cases := []struct {
		name string
		url  func(*testing.T) string
	}{
		{"tkv", func(t *testing.T) string { return "memkv://sparse-" + t.Name() }},
		{"sql", func(t *testing.T) string { return "sqlite3://" + filepath.Join(t.TempDir(), "sparse.db") }},
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		cases = append(cases, struct {
			name string
			url  func(*testing.T) string
		}{"redis", func(*testing.T) string { return redisAddr }})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewClient(tc.url(t), DefaultConf())
			if tc.name == "redis" {
				require.NoError(t, m.Reset())
			}
			require.NoError(t, m.Init(&Format{Name: "sparse", BlockSize: 4096, Compression: "none"}, true))
			t.Cleanup(func() {
				if tc.name == "redis" {
					_ = m.Reset()
				}
				_ = m.Shutdown()
			})
			ctx := Background()
			var inode Ino
			var attr Attr
			require.Zero(t, m.Create(ctx, RootInode, "f", 0644, 0, syscall.O_RDWR, &inode, &attr))
			var baseID uint64
			require.Zero(t, m.NewSlice(ctx, &baseID))
			id := baseID | SparseSliceBit
			require.Less(t, id, uint64(1<<63))
			require.Zero(t, m.Write(ctx, inode, 0, 0, Slice{Id: id, Size: 8192, Len: 11}, time.Now()))
			require.Zero(t, m.GetAttr(ctx, inode, &attr))
			require.Equal(t, uint64(11), attr.Length)
			var slices []Slice
			require.Zero(t, m.Read(ctx, inode, 0, &slices))
			require.Len(t, slices, 1)
			require.Equal(t, id, slices[0].Id)
			overlayReader, ok := m.(OverlayReader)
			require.True(t, ok)
			var overlays []OverlaySlice
			require.Zero(t, overlayReader.ReadOverlay(ctx, inode, 0, &overlays))
			require.Len(t, overlays, 1)
			require.Equal(t, id, overlays[0].Id)
			require.Zero(t, overlays[0].Pos)

			require.Zero(t, m.WriteEOF(ctx, inode, 0, 0, Slice{Id: id, Size: 8192, Len: 11}, time.Now(), 23))
			require.Zero(t, m.GetAttr(ctx, inode, &attr))
			require.Equal(t, uint64(23), attr.Length)
			require.Zero(t, overlayReader.ReadOverlay(ctx, inode, 0, &overlays))
			require.Len(t, overlays, 1)

			require.Equal(t, syscall.EINVAL,
				m.WriteEOF(ctx, inode, 0, 0, Slice{Id: id, Size: 4096, Len: 11}, time.Now(), 23))
		})
	}
}

func TestSparseBackupCounterStripsFormatBit(t *testing.T) {
	const id = uint64(12345)
	segment := &BakSegment{
		typ: segTypeChunk,
		val: &pb.Batch{Chunks: []*pb.Chunk{{
			Inode:  2,
			Index:  0,
			Slices: marshalSlice(0, id|SparseSliceBit, ChunkSize, 0, ChunkSize),
		}}},
	}
	var counters []*pb.Counter
	var dumped DumpedCounters
	require.False(t, dumped.updateFromSegment(segment, &counters))
	require.Equal(t, int64(id+1), dumped.NextChunk)
}
