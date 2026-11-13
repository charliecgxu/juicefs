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
	"os"
	"sort"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// UploadFastWriteResponse is returned by the upload-fast-write control command.
type UploadFastWriteResponse struct {
	FileCount       uint64 `json:"fileCount"`
	GenerationCount uint64 `json:"generationCount"`
	SliceCount      uint64 `json:"sliceCount"`
	ByteCount       uint64 `json:"byteCount"`
	State           string `json:"state"`
}

type fastWriteStagingFlusher interface {
	FlushStagingSlice(id uint64, size int) error
}

type fastWriteUploadTarget struct {
	inode         Ino
	maxGeneration uint64
	tier          uint8
	unlock        func()
}

type fastWriteSlice struct {
	run     fastWriteUploadRun
	sliceID uint64
	size    uint32
}

func (target *fastWriteUploadTarget) release() {
	if target != nil && target.unlock != nil {
		target.unlock()
		target.unlock = nil
	}
}

func releaseFastWriteTargets(targets []*fastWriteUploadTarget) {
	for _, target := range targets {
		target.release()
	}
}

func (v *VFS) sealFastWriteInode(ctx meta.Context, inode Ino) (*fastWriteUploadTarget, syscall.Errno) {
	if v.fastWrite == nil {
		return nil, syscall.ENOTSUP
	}
	target := &fastWriteUploadTarget{inode: inode, unlock: v.fastWrite.lockUpload(inode)}
	release := true
	defer func() {
		if release {
			target.release()
		}
	}()
	mutation := v.fastWrite.lockExistingMutation(inode)
	if mutation == nil {
		release = false
		return target, 0
	}
	defer mutation.release()
	if errno := v.checkpointFastWriteWithMutation(ctx, mutation); errno != 0 {
		return nil, errno
	}
	file := mutation.file
	file.Lock()
	generation, err := v.fastWrite.sealLocked(file)
	file.Unlock()
	if err != nil {
		return nil, syscall.EIO
	}
	target.maxGeneration = generation
	if target.maxGeneration == 0 {
		release = false
		return target, 0
	}
	var attr Attr
	if errno := v.Meta.GetAttr(ctx, inode, &attr); errno != 0 {
		return nil, errno
	}
	target.tier = attr.Tier
	release = false
	return target, 0
}

func (v *VFS) uploadFastWriteTarget(ctx meta.Context, target *fastWriteUploadTarget, stat *UploadFastWriteResponse) syscall.Errno {
	inode := target.inode
	targetGeneration := target.maxGeneration
	if targetGeneration == 0 {
		return 0
	}
	stat.FileCount++
	for {
		generation, errno := v.fastWrite.prepareUpload(inode, targetGeneration)
		if errno != 0 {
			return errno
		}
		if generation == nil {
			break
		}
		stat.GenerationCount++
		slices := make([]fastWriteSlice, 0, len(generation.Runs))
		for _, run := range generation.Runs {
			data, errno := v.fastWrite.readUploadRun(inode, generation.ID, run)
			if errno != 0 {
				return errno
			}
			var sliceID uint64
			if errno = v.Meta.NewSlice(ctx, &sliceID); errno != 0 {
				return errno
			}
			writer := v.Store.NewWriter(sliceID, target.tier)
			writer.SetWriteback(v.Conf.Chunk.Writeback)
			if _, err := writer.WriteAt(data, 0); err != nil {
				writer.Abort()
				return syscall.EIO
			}
			if err := writer.Finish(len(data)); err != nil {
				writer.Abort()
				return syscall.EIO
			}
			slices = append(slices, fastWriteSlice{run: run, sliceID: sliceID, size: uint32(len(data))})
		}
		if v.Conf.Chunk.Writeback && len(slices) > 0 {
			flusher, ok := v.Store.(fastWriteStagingFlusher)
			if !ok {
				return syscall.ENOTSUP
			}
			for _, object := range slices {
				if err := flusher.FlushStagingSlice(object.sliceID, int(object.size)); err != nil {
					logger.Errorf("flush Fast Write staging slice %d: %s", object.sliceID, err)
					return syscall.EIO
				}
			}
		}
		for _, object := range slices {
			chunkOffset := object.run.StartPage * uint32(fastWritePageSize)
			slice := meta.Slice{Id: object.sliceID, Size: object.size, Len: object.size}
			// Materializing already-visible local data must not change the
			// inode mtime, especially while a newer generation is writable.
			if errno = v.Meta.Write(ctx, inode, object.run.Chunk, chunkOffset, slice, time.Time{}); errno != 0 {
				return errno
			}
			stat.SliceCount++
			stat.ByteCount += uint64(object.size)
		}
		// Keep the local generation as a read fallback until all cached
		// remote readers have been invalidated.
		v.reader.Invalidate(inode, 0, generation.Length)
		v.invalidateAttr(inode)
		if errno = v.fastWrite.finishUpload(inode, generation.ID); errno != 0 {
			return errno
		}
	}
	return 0
}

func (v *VFS) uploadFastWriteInode(ctx meta.Context, inode Ino, stat *UploadFastWriteResponse) syscall.Errno {
	target, errno := v.sealFastWriteInode(ctx, inode)
	if errno != 0 {
		return errno
	}
	defer target.release()
	return v.uploadFastWriteTarget(ctx, target, stat)
}

func (v *VFS) collectFastWriteInodes(ctx meta.Context, paths []string) ([]Ino, syscall.Errno) {
	if len(paths) == 0 {
		if v.fastWrite == nil {
			return nil, syscall.ENOTSUP
		}
		return v.fastWrite.listInodes(), 0
	}
	filler := NewCacheFiller(v.Conf, v.Meta, v.Store)
	seen := make(map[Ino]struct{})
	var inodes []Ino
	var visit func(Ino) syscall.Errno
	visit = func(inode Ino) syscall.Errno {
		var attr Attr
		if errno := v.Meta.GetAttr(ctx, inode, &attr); errno != 0 {
			return errno
		}
		switch attr.Typ {
		case meta.TypeFile:
			if _, ok := seen[inode]; !ok {
				seen[inode] = struct{}{}
				inodes = append(inodes, inode)
			}
		case meta.TypeDirectory:
			var entries []*meta.Entry
			if errno := v.Meta.Readdir(ctx, inode, 1, &entries); errno != 0 {
				return errno
			}
			for _, entry := range entries {
				name := string(entry.Name)
				if name == "." || name == ".." {
					continue
				}
				if errno := visit(entry.Inode); errno != 0 {
					return errno
				}
			}
		}
		return 0
	}
	for _, path := range paths {
		var inode Ino
		var attr Attr
		if errno := filler.resolve(ctx, path, &inode, &attr); errno != 0 {
			return nil, errno
		}
		if errno := visit(inode); errno != 0 {
			return nil, errno
		}
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })
	return inodes, 0
}

func (v *VFS) uploadFastWritePaths(ctx meta.Context, paths []string, background bool) (*UploadFastWriteResponse, syscall.Errno) {
	stat := &UploadFastWriteResponse{State: "running"}
	if ctx.Uid() != 0 && ctx.Uid() != uint32(os.Getuid()) {
		stat.State = "failed"
		return stat, syscall.EPERM
	}
	inodes, errno := v.collectFastWriteInodes(ctx, paths)
	if errno != 0 {
		stat.State = "failed"
		return stat, errno
	}
	targets := make([]*fastWriteUploadTarget, 0, len(inodes))
	for _, inode := range inodes {
		target, sealErrno := v.sealFastWriteInode(ctx, inode)
		if sealErrno != 0 {
			releaseFastWriteTargets(targets)
			stat.State = "failed"
			return stat, sealErrno
		}
		if target.maxGeneration == 0 {
			target.release()
			continue
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		stat.State = "completed"
		return stat, 0
	}
	run := func(runCtx meta.Context, runStat *UploadFastWriteResponse) syscall.Errno {
		for index, target := range targets {
			errno := v.uploadFastWriteTarget(runCtx, target, runStat)
			target.release()
			if errno != 0 {
				releaseFastWriteTargets(targets[index+1:])
				return errno
			}
		}
		return 0
	}
	if background {
		stat.FileCount = uint64(len(targets))
		stat.State = "scheduled"
		backgroundCtx := meta.NewContext(ctx.Pid(), ctx.Uid(), ctx.Gids())
		go func() {
			if errno := run(backgroundCtx, &UploadFastWriteResponse{}); errno != 0 {
				logger.Errorf("background Fast Write upload: %s", errno)
			}
		}()
		return stat, 0
	}
	errno = run(ctx, stat)
	if errno == 0 {
		stat.State = "completed"
	} else {
		stat.State = "failed"
	}
	return stat, errno
}
