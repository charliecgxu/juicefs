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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetMaxDirtyRatioAt(t *testing.T) {
	mountpoint := filepath.Join(t.TempDir(), "mount point")
	if err := os.Mkdir(mountpoint, 0755); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "0:42")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "max_ratio")
	if err := os.WriteFile(path, []byte("1"), 0600); err != nil {
		t.Fatal(err)
	}
	mountInfo := filepath.Join(t.TempDir(), "mountinfo")
	escapedMountpoint := strings.ReplaceAll(mountpoint, " ", `\040`)
	info := "35 24 253:32 / " + escapedMountpoint + " rw - xfs /dev/vdc rw\n" +
		"36 35 0:42 / " + escapedMountpoint + " rw - fuse.juicefs JuiceFS:test rw\n"
	if err := os.WriteFile(mountInfo, []byte(info), 0600); err != nil {
		t.Fatal(err)
	}

	actual, err := setMaxDirtyRatioAt(mountpoint, 50, root, mountInfo)
	if err != nil {
		t.Fatal(err)
	}
	if actual != 50 {
		t.Fatalf("actual max ratio is %d, want 50", actual)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "50" {
		t.Fatalf("max ratio is %q, want 50", got)
	}
}

func TestSetMaxDirtyRatioAtRejectsInvalidRatio(t *testing.T) {
	for _, ratio := range []int{-1, 0, 101} {
		if _, err := setMaxDirtyRatioAt(t.TempDir(), ratio, t.TempDir(), "missing"); err == nil {
			t.Fatalf("ratio %d should fail", ratio)
		}
	}
}
