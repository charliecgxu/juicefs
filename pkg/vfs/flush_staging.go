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
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// FlushStagingResponse is returned to the control client.
type FlushStagingResponse struct {
	FileCount  uint64 `json:"fileCount"`
	SliceCount uint64 `json:"sliceCount"`
	BlockCount uint64 `json:"blockCount"`
	Async      bool   `json:"async"`
}

type stagingFile struct {
	inode  Ino
	length uint64
}

// flushStagingPaths force-uploads staging blocks for the given paths.
// Empty paths flush the entire staging area.
//
// When background is true (async): resolve files, flush their writers into
// staging, and queue object uploads, then return without waiting for Put.
// When background is false: also wait until those staging blocks are uploaded.
func (v *VFS) flushStagingPaths(ctx meta.Context, paths []string, background bool) (*FlushStagingResponse, syscall.Errno) {
	stat := &FlushStagingResponse{Async: background}
	waitUpload := !background

	if len(paths) == 0 {
		logger.Infof("flush staging: all blocks (async=%v)", background)
		if background {
			go func() {
				if err := v.Store.FlushStaging(); err != nil {
					logger.Errorf("flush staging (all): %s", err)
				}
			}()
			return stat, 0
		}
		if err := v.Store.FlushStaging(); err != nil {
			logger.Errorf("flush staging (all): %s", err)
			return stat, syscall.EIO
		}
		return stat, 0
	}

	files := v.collectStagingFiles(ctx, paths)
	objs := make([]chunk.StagingObject, 0, 64)
	seen := make(map[uint64]struct{})
	for _, f := range files {
		_ = v.writer.Flush(ctx, f.inode)
		stat.FileCount++
		nChunks := (f.length + meta.ChunkSize - 1) / meta.ChunkSize
		for i := uint64(0); i < nChunks; i++ {
			var slices []meta.Slice
			if st := v.Meta.Read(ctx, f.inode, uint32(i), &slices); st != 0 {
				logger.Warnf("flush staging: read inode %d chunk %d: %s", f.inode, i, st)
				continue
			}
			for _, s := range slices {
				if s.Id == 0 || s.Size == 0 {
					continue
				}
				if _, ok := seen[s.Id]; ok {
					continue
				}
				seen[s.Id] = struct{}{}
				objs = append(objs, chunk.StagingObject{Id: s.Id, Size: int(s.Size)})
				stat.SliceCount++
			}
		}
	}

	logger.Infof("flush staging: %d files, %d slices, async=%v", stat.FileCount, stat.SliceCount, background)
	queued, err := v.Store.FlushStagingObjects(objs, waitUpload)
	if err != nil {
		logger.Errorf("flush staging objects: %s", err)
		return stat, syscall.EIO
	}
	stat.BlockCount = uint64(queued)
	return stat, 0
}

func (v *VFS) collectStagingFiles(ctx meta.Context, paths []string) []stagingFile {
	filler := NewCacheFiller(v.Conf, v.Meta, v.Store)
	var files []stagingFile
	for _, p := range paths {
		var inode Ino
		var attr Attr
		if st := filler.resolve(ctx, p, &inode, &attr); st != 0 {
			logger.Warnf("flush staging: resolve %q: %s", p, st)
			continue
		}
		switch attr.Typ {
		case meta.TypeFile:
			files = append(files, stagingFile{inode, attr.Length})
		case meta.TypeDirectory:
			files = append(files, v.listFilesUnder(ctx, inode)...)
		default:
			logger.Warnf("flush staging: skip non-file %q (type %d)", p, attr.Typ)
		}
	}
	return files
}

func (v *VFS) listFilesUnder(ctx meta.Context, root Ino) []stagingFile {
	var out []stagingFile
	pending := []Ino{root}
	for len(pending) > 0 {
		ino := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		var entries []*meta.Entry
		if st := v.Meta.Readdir(ctx, ino, 1, &entries); st != 0 {
			logger.Warnf("flush staging: readdir %d: %s", ino, st)
			continue
		}
		for _, e := range entries {
			name := string(e.Name)
			if name == "." || name == ".." {
				continue
			}
			switch e.Attr.Typ {
			case meta.TypeDirectory:
				pending = append(pending, e.Inode)
			case meta.TypeFile:
				out = append(out, stagingFile{e.Inode, e.Attr.Length})
			}
		}
	}
	return out
}
