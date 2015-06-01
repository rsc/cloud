// Copyright 2015 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package metaflag sets command-line flags according to the
// metadata specified in a Google Compute Engine instance creation request.
package metaflag

import (
	"flag"
	"log"

	"google.golang.org/cloud/compute/metadata"
)

// Init looks up each registered command-line flag in the
// Google Compute Engine instance metadata server to see
// if a value was specified in the instance creation request.
// If so, Init sets the flag to that value.
//
// For example, if the instance request said --metadata x=y and there is
// a flag named x, metaflag.Init causes the flag x to be set to y.
//
// The expected use is:
//
//	metadata.Init()
//	flag.Parse()
//
// to allow the instance creation request to set default flag values but still allow
// the command line to override those.
func Init() {
	println("METAFLAG")
	flag.VisitAll(func(f *flag.Flag) {
		val, err := metadata.InstanceAttributeValue(f.Name)
		if err != nil {
			println("GET", f.Name, "=>", err.Error())
		}
		if err == nil {
			println("GET", f.Name, "=>", val)
			if err := flag.Set(f.Name, val); err != nil {
				log.Fatal(err)
			}
		}
	})
}
