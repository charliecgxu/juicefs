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

func cmdFlushStaging() *cli.Command {
	return &cli.Command{
		Name:      "flush-staging",
		Aliases:   []string{"flush2"},
		Action:    flushStaging,
		Category:  "TOOL",
		Usage:     "Force upload writeback staging blocks for files",
		ArgsUsage: "PATH ...",
		Description: `
Force-upload local writeback staging blocks belonging to the given files or
directories. By default the request is asynchronous: the command returns after
the work is scheduled, without waiting for object-storage Put to finish.

With no PATH (or only the mount point), all staging blocks are flushed.

Examples:
# Async flush one file (default)
$ juicefs flush-staging /mnt/jfs/data/a.bin

# Async flush a directory tree
$ juicefs flush-staging /mnt/jfs/data/

# Wait until uploads finish
$ juicefs flush-staging --wait /mnt/jfs/data/a.bin

# Flush everything under the volume (async)
$ juicefs flush2 /mnt/jfs`,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "background",
				Aliases: []string{"b"},
				Value:   true,
				Usage:   "run asynchronously (default true)",
			},
			&cli.BoolFlag{
				Name:  "wait",
				Usage: "wait until staging uploads finish (overrides --background)",
			},
		},
	}
}

func flushStaging(ctx *cli.Context) error {
	setup0(ctx, 1, 0)
	args := ctx.Args().Slice()
	if len(args) == 0 {
		return fmt.Errorf("PATH is required")
	}

	var absPaths []string
	for _, p := range args {
		abs, err := filepath.Abs(p)
		if err != nil {
			return fmt.Errorf("abs path %q: %s", p, err)
		}
		absPaths = append(absPaths, abs)
	}

	controller, err := openController(absPaths[0])
	if err != nil {
		return fmt.Errorf("open control file for %s: %s", absPaths[0], err)
	}
	defer controller.Close()

	mp := absPaths[0]
	for ; mp != "/"; mp = filepath.Dir(mp) {
		inode, err := utils.GetFileInode(mp)
		if err != nil {
			return fmt.Errorf("lookup inode for %q: %s", mp, err)
		}
		if inode == uint64(meta.RootInode) {
			break
		}
	}

	// Rewrite to volume-relative paths. Mount point alone → empty list = flush all.
	var rel []string
	for _, p := range absPaths {
		if p == mp || p == mp+"/" {
			continue
		}
		if mp == "/" {
			ino, err := utils.GetFileInode(p)
			if err != nil {
				logger.Warnf("lookup %q: %s", p, err)
				continue
			}
			rel = append(rel, fmt.Sprintf("inode:%d", ino))
			continue
		}
		if !strings.HasPrefix(p, mp+"/") && p != mp {
			logger.Warnf("skip %q: not under mount point %s", p, mp)
			continue
		}
		rel = append(rel, strings.TrimPrefix(p, mp))
	}

	background := ctx.Bool("background") && !ctx.Bool("wait")
	pathsBlob := strings.Join(rel, "\n")
	bodyLen := uint32(4 + len(pathsBlob) + 1)
	wb := utils.NewBuffer(8 + bodyLen)
	wb.Put32(meta.FlushStaging)
	wb.Put32(bodyLen)
	wb.Put32(uint32(len(pathsBlob)))
	wb.Put([]byte(pathsBlob))
	if background {
		wb.Put8(1)
	} else {
		wb.Put8(0)
	}

	if _, err := controller.Write(wb.Bytes()); err != nil {
		return fmt.Errorf("write flush-staging command: %s", err)
	}

	start := time.Now()
	data, errno := readProgress(controller, func(_, _ uint64) {})
	if errno != 0 {
		return fmt.Errorf("flush-staging failed: %s", syscall.Errno(errno))
	}
	var resp vfs.FlushStagingResponse
	if len(data) > 0 {
		if err := json.Unmarshal(data, &resp); err != nil {
			return fmt.Errorf("decode response: %s", err)
		}
	}
	if background || resp.Async {
		fmt.Printf("flush-staging scheduled (async): files=%d slices=%d paths=%d\n",
			resp.FileCount, resp.SliceCount, len(rel))
		return nil
	}
	fmt.Fprintf(os.Stdout, "flush-staging done in %s: files=%d slices=%d blocks=%d\n",
		time.Since(start).Truncate(time.Millisecond), resp.FileCount, resp.SliceCount, resp.BlockCount)
	return nil
}
