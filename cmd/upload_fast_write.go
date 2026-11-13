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

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

func cmdUploadFastWrite() *cli.Command {
	return &cli.Command{
		Name:      "upload-fast-write",
		Action:    uploadFastWrite,
		Category:  "TOOL",
		Usage:     "Materialize unpublished Fast Write data into normal slices",
		ArgsUsage: "PATH ...",
		Description: `Materialize Fast Write generations into the existing dense slice format.
All selected inodes are checkpointed and sealed before the command returns.
The default uploads those fixed generations asynchronously. Use --wait before
publish to wait for slice materialization and object visibility.`,
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "wait", Usage: "wait for slice materialization and object upload"},
		},
	}
}

func uploadFastWrite(ctx *cli.Context) error {
	setup0(ctx, 1, 0)
	args := ctx.Args().Slice()
	if len(args) == 0 {
		return fmt.Errorf("PATH is required")
	}
	absPaths := make([]string, 0, len(args))
	for _, path := range args {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("abs path %q: %s", path, err)
		}
		absPaths = append(absPaths, absolute)
	}
	controller, err := openController(absPaths[0])
	if err != nil {
		return fmt.Errorf("open control file for %s: %s", absPaths[0], err)
	}
	defer controller.Close()

	mountpoint := absPaths[0]
	for ; mountpoint != "/"; mountpoint = filepath.Dir(mountpoint) {
		inode, err := utils.GetFileInode(mountpoint)
		if err != nil {
			return fmt.Errorf("lookup inode for %q: %s", mountpoint, err)
		}
		if inode == uint64(meta.RootInode) {
			break
		}
	}
	var relative []string
	for _, path := range absPaths {
		if path == mountpoint || path == mountpoint+"/" {
			continue
		}
		if mountpoint == "/" {
			inode, err := utils.GetFileInode(path)
			if err != nil {
				return fmt.Errorf("lookup inode for %q: %s", path, err)
			}
			relative = append(relative, fmt.Sprintf("inode:%d", inode))
			continue
		}
		if !strings.HasPrefix(path, mountpoint+"/") {
			return fmt.Errorf("%q is not under mount point %s", path, mountpoint)
		}
		relative = append(relative, strings.TrimPrefix(path, mountpoint))
	}
	background := !ctx.Bool("wait")
	request, err := encodeUploadFastWriteRequest(relative, background)
	if err != nil {
		return err
	}
	if _, err = controller.Write(request); err != nil {
		return fmt.Errorf("write upload-fast-write command: %s", err)
	}
	started := time.Now()
	data, errno := readProgress(controller, func(_, _ uint64) {})
	if errno != 0 {
		if syscall.Errno(errno) == syscall.EINVAL {
			return fmt.Errorf("upload-fast-write is not supported by this mount; upgrade and remount it")
		}
		return fmt.Errorf("upload-fast-write failed: %s", syscall.Errno(errno))
	}
	var response vfs.UploadFastWriteResponse
	if len(data) > 0 {
		if err = json.Unmarshal(data, &response); err != nil {
			return fmt.Errorf("decode upload-fast-write response: %s", err)
		}
	}
	if response.State == "scheduled" {
		fmt.Fprintf(os.Stdout, "upload-fast-write sealed and scheduled: files=%d\n", response.FileCount)
		return nil
	}
	fmt.Fprintf(os.Stdout, "upload-fast-write done in %s: files=%d generations=%d slices=%d bytes=%d\n",
		time.Since(started).Truncate(time.Millisecond), response.FileCount, response.GenerationCount,
		response.SliceCount, response.ByteCount)
	return nil
}

func encodeUploadFastWriteRequest(paths []string, background bool) ([]byte, error) {
	bodyLength := uint64(4 + 1)
	for _, path := range paths {
		bodyLength += 4 + uint64(len(path))
	}
	const maxControlMessageSize = uint64(^uint32(0))
	if bodyLength+8 > maxControlMessageSize {
		return nil, fmt.Errorf("upload-fast-write request is too large")
	}
	request := utils.NewBuffer(uint32(8 + bodyLength))
	request.Put32(meta.UploadFastWrite)
	request.Put32(uint32(bodyLength))
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
	return request.Bytes(), nil
}
