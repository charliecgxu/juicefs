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

	"github.com/juicedata/juicefs/pkg/meta"
)

// FlushStagingResponse is returned to the control client.
type FlushStagingResponse struct {
	FileCount  uint64 `json:"fileCount"`
	SliceCount uint64 `json:"sliceCount"`
	Async      bool   `json:"async"`
}

type stagingFile struct {
	inode Ino
}

// flushStagingPaths publishes sparse generations for the given paths.
// Empty paths publish the entire sparse staging area.
//
// When background is true, the Put is scheduled asynchronously.
func (v *VFS) flushStagingPaths(ctx meta.Context, paths []string, background bool) (*FlushStagingResponse, syscall.Errno) {
	stat := &FlushStagingResponse{Async: background}
	if v.sparse == nil {
		logger.Errorf("flush staging requires sparse-chunk-slice")
		return stat, syscall.ENOTSUP
	}

	if len(paths) == 0 {
		logger.Infof("publish sparse staging: all generations (async=%v)", background)
		if background {
			go func() {
				if _, err := v.sparse.PublishAll(); err != nil {
					logger.Errorf("flush staging (all): %s", err)
				}
			}()
			return stat, 0
		}
		n, err := v.sparse.PublishAll()
		stat.SliceCount = uint64(n)
		if err != nil {
			logger.Errorf("publish sparse staging (all): %s", err)
			return stat, syscall.EIO
		}
		return stat, 0
	}

	files := v.collectStagingFiles(ctx, paths)
	inodes := make([]meta.Ino, 0, len(files))
	for _, f := range files {
		inodes = append(inodes, f.inode)
		_ = v.writer.Flush(ctx, f.inode)
		stat.FileCount++
	}
	if background {
		go func() {
			if _, err := v.sparse.PublishInodes(inodes); err != nil {
				logger.Errorf("publish sparse staging: %s", err)
			}
		}()
	} else {
		n, err := v.sparse.PublishInodes(inodes)
		stat.SliceCount = uint64(n)
		if err != nil {
			logger.Errorf("publish sparse staging: %s", err)
			return stat, syscall.EIO
		}
	}
	logger.Infof("publish sparse staging: %d files, %d slices, async=%v", stat.FileCount, stat.SliceCount, background)
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
			files = append(files, stagingFile{inode})
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
				out = append(out, stagingFile{e.Inode})
			}
		}
	}
	return out
}
