// Copyright 2015 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gcs implements diskcache.Loader using Google Cloud Storage.
package gcs

import (
	"fmt"
	"io"
	"net/http"
	"os"
	pathpkg "path"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"rsc.io/cloud/diskcache"
)

const scopeReadOnly = "https://www.googleapis.com/auth/devstorage.read_only"

func NewLoader(root string) (diskcache.Loader, error) {
	client, err := google.DefaultClient(oauth2.NoContext, scopeReadOnly)
	if err != nil {
		return nil, err
	}
	return NewLoaderWithClient(client, root), nil
}

func NewLoaderWithClient(client *http.Client, root string) diskcache.Loader {
	l := &loader{
		client: client,
		root:   root,
	}
	return l
}

type loader struct {
	client *http.Client
	root   string
}

func (l *loader) Load(path string, target *os.File, meta []byte) (cacheValid bool, newMeta []byte, err error) {
	path = pathpkg.Join("/", l.root, path)[1:]
	println("LOAD", path)
	defer func() {
		if err != nil {
			println("LOAD ERROR", err.Error())
		}
	}()
	i := strings.Index(path, "/")
	if i < 0 {
		return false, nil, fmt.Errorf("path too short")
	}

	// NOTE(rsc): It's tempting to use the JSON API v1 instead of the XML API,
	// just on general principle, but the URL encoding is different.
	// For gs://swtch/web/index.html, the XML API URL is
	//	https://storage.googleapis.com/swtch/web/index.html
	// but the JSON API v1 URL for the data is
	//	https://www.googleapis.com/storage/v1/b/swtch/o/web%2Findex.html?alt=media
	// There's no body here, just the raw content, so XML vs JSON doesn't actually matter to us.
	// We use the one with the simpler URL.
	// Also I seem to get 404s when I try even the correct JSON URL here,
	// although somehow not from curl. This is clearly a giant mess.
	// There may be an escaping problem lurking here even with the XML API. Not clear.

	url := "https://storage.googleapis.com/" + path
	println("URL", url)
	req, err := http.NewRequest("GET", url, nil)
	if len(meta) > 0 {
		req.Header.Set("If-None-Match", string(meta))
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return false, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 304 {
		return true, meta, nil
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 404 {
			return false, nil, &os.PathError{Path: path, Op: "read", Err: os.ErrNotExist}
		}
		return false, nil, &os.PathError{Path: path, Op: "read", Err: fmt.Errorf("%s", resp.Status)}
	}

	// TODO(rsc): Maybe work harder with range requests to restart interrupted transfers.
	_, err = io.Copy(target, resp.Body)
	if err != nil {
		return false, nil, err
	}

	meta = []byte(resp.Header.Get("Etag"))
	return false, meta, nil
}
