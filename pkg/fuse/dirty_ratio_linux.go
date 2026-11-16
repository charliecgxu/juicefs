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

package fuse

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	bdiClassPath  = "/sys/class/bdi"
	mountInfoPath = "/proc/self/mountinfo"
)

func setMaxDirtyRatio(mountpoint string, ratio int) (int, error) {
	return setMaxDirtyRatioAt(mountpoint, ratio, bdiClassPath, mountInfoPath)
}

func setMaxDirtyRatioAt(mountpoint string, ratio int, bdiRoot, mountInfo string) (int, error) {
	if ratio < 1 || ratio > 100 {
		return 0, fmt.Errorf("dirty ratio %d is outside [1, 100]", ratio)
	}
	bdi, err := findFuseDevice(mountpoint, mountInfo)
	if err != nil {
		return 0, err
	}
	path := filepath.Join(bdiRoot, bdi, "max_ratio")
	if err := os.WriteFile(path, []byte(strconv.Itoa(ratio)), 0600); err != nil {
		return 0, fmt.Errorf("write %s: %w", path, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read back %s: %w", path, err)
	}
	actual, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("parse %s value %q: %w", path, data, err)
	}
	return actual, nil
}

func findFuseDevice(mountpoint, mountInfo string) (string, error) {
	mountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		return "", fmt.Errorf("resolve mountpoint: %w", err)
	}
	f, err := os.Open(mountInfo)
	if err != nil {
		return "", fmt.Errorf("open mountinfo: %w", err)
	}
	defer f.Close()

	var device string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 7 || unescapeMountInfo(fields[4]) != mountpoint {
			continue
		}
		for i, field := range fields[6:] {
			if field == "-" && i+7 < len(fields) && strings.HasPrefix(fields[i+7], "fuse") {
				device = fields[2]
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan mountinfo: %w", err)
	}
	if device == "" {
		return "", fmt.Errorf("FUSE mount %q not found in mountinfo", mountpoint)
	}
	return device, nil
}

func unescapeMountInfo(path string) string {
	return strings.NewReplacer(
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
		`\134`, `\`,
	).Replace(path)
}
