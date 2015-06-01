// Copyright 2015 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package diskcache implements an on-disk cache of a remote file tree.
//
// A cache, of type *Cache, manages a local directory holding cached copies
// of remote files obtained from a loader, of type Loader. The loader handles
// reading remote files and checking whether a cached copy is still valid.
//
// To open a file, the cache first searches the local directory for a cached copy.
// After being revalidated or fetched by the loader, a copy is considered
// to be valid for a fixed duration, after which it is kept but considered expired.
// If a copy exists and has not expired, the cache uses it.
// If no copy exists or the copy has expired, the cache invokes the
// loader to refresh the copy or fetch a new copy.
//
// A cached file is checked for expiration only during open, not during
// individual reads.
//
// A cache may be used by multiple goroutines simultaneously.
// Furthermore, multiple caches, even in separate processes,
// may share a single directory, provided the caches use the same loader.
// In this way, a cache can be reused across multiple program executions
// and also be shared by concurrently executing programs.
// If a cache directory is to be shared by concurrent programs,
// it should usually be on local disk, because Unix file locking over
// network file systems is almost always broken.
//
// There is no cache for file load errors.
//
// On-Disk Format
//
// Each cached file stored on disk using a name derived from the
// SHA1 hash of the file name. The first three hex digits name a
// subdirectory of the cache root directory, and the remaining
// seventeen digits are used as the base name of a group of files
// within that subdirectory:
//
//	123/45678901234567890.data
//	123/45678901234567890.meta
//	123/45678901234567890.used
//	123/45678901234567890.next
//
// The .data file is the cached file content. If it exists, it is a complete copy,
// never a partial one.
//
// The .meta file is the metadata associated with the .data file.
// It contains the JSON encoding of a metadata struct.
// The modification time of the .meta file is the time that the .data file
// was last downloaded or revalidated. The .data file is considered to
// be valid until that time plus the expiration period.
// As a special case, if the .meta file has a modification time of
// January 1, 1970 00:00:00 UTC (Unix time 0), the .data file is
// considered expired, even if there is no expiration period.
//
// The .used file holds a single \n byte. It is rewritten each time
// the .data file is opened to satisfy a file open operation.
// The modification time of the .used file is therefore the time of the
// last use of the file.
//
// The .next file holds the next version of the cached file, while it is
// being downloaded. Once the download has completed, the cache
// renames the .next file onto the .data file. This sequence avoids
// overwriting the content of the .data file, which other clients
// might still be reading.
//
// To allow multiple instances of a cache to manage a shared directory,
// if a cache is doing the initial download of a file or revalidating
// an expired copy or redownloading a new copy, it must hold an
// exclusive BSD file lock on the .meta file (using flock(2)).
//
// After downloading a new file and installing it as a .data file, the
// cache must check that it has not exceeded the on-disk size limit.
// It checks by reading the sizes of all the .data files in the directory
// tree and the modification times of the .used files.
// It then removes the oldest cached files (.data, .meta, and .used)
// until the data files again fit within the limit. To remove a file,
// the cache must hold the .meta file lock.
//
// Warning Warning Warning
//
// This package is unfinished. In particular, DeleteAll and ExpireAll are unimplemented,
// as is the code to delete files to stay within the maximum data size limit.
//
package diskcache

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	pathpkg "path"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

// A Cache provides read-only access to a remote file tree,
// caching opened files on local disk.
type Cache struct {
	dir    string
	loader Loader

	atomicExpiration int64
	atomicMaxData    int64
}

// Loader is the interface Cache uses to load remote file content.
//
// The Load method fetches path from the remote location, writing it to target.
// If the cache already has a (possibly expired) copy of the file, meta will be
// the metadata returned by a previous call to Load. Otherwise meta is nil.
// If the cached copy is still valid, Load should return cacheValid==true,
// newMeta==meta (or an updated version), and err==nil.
// Otherwise, Load should fetch the data, write it to target, and return
// cacheValid==true, a new metadata block in newMeta, and err==nil.
//
// The elements in a file path are separated by slash ('/', U+002F)
// characters, regardless of host operating system convention.
type Loader interface {
	Load(path string, target *os.File, meta []byte) (cacheValid bool, newMeta []byte, err error)
}

// metaDisk is the on-disk metadata storage format
type metaDisk struct {
	Path        string
	CreateTime  time.Time
	RefreshTime time.Time
	Load        []byte
}

// New returns a new Cache that reads files from loader,
// caching at most max bytes in the directory dir.
// If dir does not exist, New will attempt to create it.
func New(dir string, loader Loader) (*Cache, error) {
	// Create dir if necessary.
	fi, err := os.Stat(dir)
	if err != nil || !fi.IsDir() {
		if err := os.Mkdir(dir, 0777); err != nil {
			return nil, err
		}
	}

	c := &Cache{
		dir:    dir,
		loader: loader,
	}
	return c, nil
}

// SetExpiration sets the duration after which a cached copy is
// considered to have expired.
// If the duration d is zero (the default), cached copies never expire.
func (c *Cache) SetExpiration(d time.Duration) {
	atomic.StoreInt64(&c.atomicExpiration, int64(d))
}

func (c *Cache) expiration() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.atomicExpiration))
}

// SetMaxData sets the maximum bytes of data to hold in cached copies.
// The limit is imposed in a best effort fashion.
// In particular, it does not apply to old copies that have not yet been closed,
// nor to new copies that have not finished downloading,
// nor to cache metadata.
func (c *Cache) SetMaxData(max int64) {
	atomic.StoreInt64(&c.atomicMaxData, max)
}

func (c *Cache) maxData() int64 {
	return atomic.LoadInt64(&c.atomicMaxData)
}

func (c *Cache) locate(path string) (cleaned, prefix string) {
	cleaned = pathpkg.Clean("/" + path)
	sum := sha1.Sum([]byte(cleaned))
	h := fmt.Sprintf("%x", sum[:])
	parent := filepath.Join(c.dir, h[0:3])
	os.Mkdir(parent, 0777)
	return cleaned, filepath.Join(c.dir, h[0:3], h[3:])
}

func (c *Cache) metaLock(prefix string) (*os.File, error) {
	name := prefix + ".meta"
	f, err := os.OpenFile(name, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// Open opens the file with the given path.
// The caller is responsible for closing the returned file when finished with it.
// The elements in a file path are separated by slash ('/', U+002F)
// characters, regardless of host operating system convention.
func (c *Cache) Open(path string) (*os.File, error) {
	path, prefix := c.locate(path)

	// Fast path: if not expired and data file exists, done.
	fi, err := os.Stat(prefix + ".meta")
	d := c.expiration()
	if err == nil && (d == 0 || time.Now().Before(fi.ModTime().Add(d))) {
		if data, err := os.Open(prefix + ".data"); err == nil {
			return data, nil
		}
	}

	// Otherwise lock .meta file, creating it if necessary.
	metaFile, err := c.metaLock(prefix)
	if err != nil {
		f, errCreate := os.OpenFile(prefix+".meta", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if errCreate == nil {
			f.Close()
		}
		metaFile, err = c.metaLock(prefix)
		if err != nil {
			if errCreate != nil {
				return nil, fmt.Errorf("creating metadata file: %v", errCreate)
			}
			return nil, err
		}
	}
	defer metaFile.Close()

	// Double-check expiration.
	// We hold the meta lock, so nothing should change underfoot.
	fi, err = metaFile.Stat()
	if err != nil {
		metaFile.Close()
		return nil, fmt.Errorf("stat'ing metadata file: %v", err)
	}
	data, errData := os.Open(prefix + ".data")
	if (d == 0 || time.Now().Before(fi.ModTime().Add(d))) && errData == nil {
		return data, nil
	}
	if errData == nil {
		data.Close()
	}
	defer metaFile.Close()

	// Read metadata.
	js, err := ioutil.ReadAll(metaFile)
	if err != nil {
		// TODO(rsc): Delete?
		return nil, fmt.Errorf("reading metadata file: %v", err)
	}
	var meta metaDisk
	if len(js) > 0 {
		if err := json.Unmarshal(js, &meta); err != nil {
			// TODO(rsc): Delete?
			return nil, fmt.Errorf("reading metadata file: %v", err)
		}
	}

	if errData != nil {
		os.Remove(prefix + ".data")
		meta.Load = nil
	}

	next, err := os.OpenFile(prefix+".next", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		// Shouldn't happen, but maybe there is a stale .next file. Remove and try again.
		os.Remove(prefix + ".next")
		next, err = os.OpenFile(prefix+".next", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return nil, fmt.Errorf("creating cached file: %v", err)
		}
	}

	cacheValid, metaLoad, err := c.loader.Load(path, next, meta.Load)
	if err != nil {
		next.Close()
		return nil, err
	}

	meta.RefreshTime = time.Now()
	var nextSize int64
	if cacheValid {
		next.Close()
		os.Remove(prefix + ".next")
	} else {
		meta.CreateTime = meta.RefreshTime
		fi, err := next.Stat()
		if err != nil {
			return nil, fmt.Errorf("writing cached file: %v", err)
		}
		nextSize = fi.Size()
		if err := next.Close(); err != nil {
			return nil, fmt.Errorf("writing cached file: %v", err)
		}
		if err := os.Rename(prefix+".next", prefix+".data"); err != nil {
			// Shouldn't happen, but we did get the file. Use it.
			return nil, fmt.Errorf("installing cached file: %v", err)
		}
	}

	meta.Load = metaLoad
	meta.Path = path
	js, err = json.Marshal(&meta)
	if err != nil {
		return nil, fmt.Errorf("preparing meta file: %v", err)
	}

	// Use WriteFile instead of metaFile.Write in order to force
	// truncation of the meta file when the new JSON is less than the old JSON.
	if err := ioutil.WriteFile(prefix+".meta", []byte(js), 0666); err != nil {
		// Unclear what state we are in now.
		// The write succeeded but close failed.
		// Cache is supposed to be on local disk,
		// so this should not be possible.
		// Hope for the best.
		_ = err
	}

	// We'd prefer to return the file named .data, not .next. Try.
	data, err = os.Open(prefix + ".data")
	if err != nil {
		return nil, err
	}
	metaFile.Close()

	if nextSize > 0 {
		c.checkDataLimit(nextSize)
	}

	return data, nil
}

func (c *Cache) ReadFile(path string) ([]byte, error) {
	f, err := c.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}

func (c *Cache) checkDataLimit(newSize int64) {
}

// Delete deletes the cache entry for the file with the given path.
func (c *Cache) Delete(path string) error {
	path, prefix := c.locate(path)
	metaFile, err := c.metaLock(prefix)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	os.Remove(prefix + ".data")
	os.Remove(prefix + ".next")
	os.Remove(prefix + ".used")
	err = os.Remove(prefix + ".meta")
	metaFile.Close()
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// DeleteAll deletes all the cache entries.
func (c *Cache) DeleteAll() error {
	panic("not implemented")
}

// Expire marks the cache entry for the file with the given path as expired.
// The cache will have to revalidate the local copy, if any, before using it again.
func (c *Cache) Expire(path string) error {
	path, prefix := c.locate(path)
	t := time.Unix(0, 0)
	err := os.Chtimes(prefix+".meta", t, t)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ExpireAll marks all cache entries as expired.
func (c *Cache) ExpireAll() error {
	panic("not implemented")
}
