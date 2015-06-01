// Copyright 2015 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package diskcache

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"
)

func newCache(t *testing.T, loader Loader) (c *Cache, cleanup func()) {
	dir, err := ioutil.TempDir("", "diskcache-test-")
	if err != nil {
		t.Fatal(err)
	}
	cleanup = func() { os.RemoveAll(dir) }
	c, err = New(dir+"/cache", loader)
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	return c, cleanup
}

func readFile(t *testing.T, c *Cache, name string) []byte {
	f, err := c.Open(name)
	if err != nil {
		t.Fatalf("readFile %s: %v", name, err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("readFile %s: %v", name, err)
	}
	return data
}

type loaderFunc func(string, *os.File, []byte) (bool, []byte, error)

func (f loaderFunc) Load(path string, target *os.File, meta []byte) (cacheValid bool, newMeta []byte, err error) {
	return f(path, target, meta)
}

func loadHello(path string, target *os.File, meta []byte) (bool, []byte, error) {
	n, _ := strconv.Atoi(string(meta))
	n++

	fmt.Fprintf(target, "hello, %s #%d\n", path, n)
	return false, []byte(fmt.Sprint(n)), nil
}

func TestBasic(t *testing.T) {
	c, cleanup := newCache(t, loaderFunc(loadHello))
	defer cleanup()

	const first = "hello, /file #1\n"
	if data := readFile(t, c, "file"); string(data) != first {
		t.Fatalf("original read file = %q, want %q", data, first)
	}
	if data2 := readFile(t, c, "file"); string(data2) != first {
		t.Fatalf("cached read file = %q, want %q", data2, first)
	}

	const second = "hello, /file #2\n"
	c.SetExpiration(1 * time.Nanosecond)
	if data3 := readFile(t, c, "file"); string(data3) != second {
		t.Fatalf("expired read file = %q, want %q", data3, second)
	}
	const third = "hello, /file #3\n"
	if data4 := readFile(t, c, "file"); string(data4) != third {
		t.Fatalf("recached read file = %q, want %q", data4, third)
	}
	c.SetExpiration(0)
	if data5 := readFile(t, c, "file"); string(data5) != third {
		t.Fatalf("recached read file = %q, want %q", data5, third)
	}
}
