// Copyright 2015 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"rsc.io/cloud/diskcache"
	"rsc.io/cloud/google/gcs"
)

var (
	cache      *diskcache.Cache
	exitStatus int
)

var (
	flagExpire = flag.Duration("expire", 0, "expiration interval")
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: gcscat bucket/path ...\n")
	os.Exit(2)
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("gcscat: ")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
	}

	loader, err := gcs.NewLoader("/")
	cache, err = diskcache.New("/tmp/gcscache", loader)
	if *flagExpire != 0 {
		cache.SetExpiration(*flagExpire)
	}
	if err != nil {
		log.Fatal(err)
	}

	for _, arg := range flag.Args() {
		cat(arg)
	}
	os.Exit(exitStatus)
}

func cat(arg string) {
	f, err := cache.Open(arg)
	if err != nil {
		log.Print(err)
		exitStatus = 1
		return
	}
	if _, err := io.Copy(os.Stdout, f); err != nil {
		exitStatus = 1
	}
	f.Close()
}
