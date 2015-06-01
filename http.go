// Package cloud provides utilities for writing simple cloud-based web servers.
//
// Most of the heavy lifting is done by packages in subdirectories.
// Package cloud itself mainly implements connections to the
// standard library and other interfaces.
//
// This entire repo is but the draft of a draft. It exists to support the swtch.com web server.
// It may mature into something more general, or it may not.
//
package cloud

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"rsc.io/cloud/diskcache"
)

// Dir returns an http.FileSystem corresponding to the cached file subtree rooted at dir.
//
// A typical use of Dir is to pass to http.FileServer to create an HTTP handler
// serving files from a cached subtree:
//
//	http.Handle("/static/", http.StripPrefix("/static", http.FileServer(cloud.Dir(cache, "/myfiles"))))
//
func Dir(cache *diskcache.Cache, dir string) http.FileSystem {
	return &fileSystem{cache, dir}
}

type fileSystem struct {
	c    *diskcache.Cache
	root string
}

func (fs *fileSystem) Open(path string) (http.File, error) {
	if strings.Contains(path, "/cgi-bin/") || strings.Contains(path, "/.") {
		return nil, &os.PathError{Path: path, Op: "open", Err: os.ErrNotExist}
	}
	f, err := fs.c.Open(fs.root + "/" + path)
	if err != nil {
		// File doesn't exist, but might be a directory.
		// If index.html exists, return an empty directory.
		// That's enough for the http server to try to open index.html.
		if f, err1 := fs.c.Open(fs.root + "/" + path + "/index.html"); err1 == nil {
			f.Close()
			return &emptyDir{}, nil
		}
		log.Printf("cloud.Dir: open %s: %v", path, err)
		return nil, err
	}
	return f, nil
}

type emptyDir struct{}

func (*emptyDir) Close() error                                 { return nil }
func (*emptyDir) Read([]byte) (int, error)                     { return 0, io.EOF }
func (*emptyDir) Readdir(count int) ([]os.FileInfo, error)     { return nil, io.EOF }
func (*emptyDir) Seek(offset int64, whence int) (int64, error) { return 0, nil }
func (*emptyDir) Stat() (os.FileInfo, error)                   { return &dirInfo{}, nil }

type dirInfo struct{}

func (*dirInfo) Name() string       { return "/" }
func (*dirInfo) Size() int64        { return 0 }
func (*dirInfo) Mode() os.FileMode  { return os.ModeDir | 0555 }
func (*dirInfo) ModTime() time.Time { return time.Now() }
func (*dirInfo) IsDir() bool        { return true }
func (*dirInfo) Sys() interface{}   { return nil }

// LoadX509KeyPair is crypto/tls's LoadX509KeyPair, but it reads the key pair from cache instead of local disk.
func LoadX509KeyPair(cache *diskcache.Cache, certFile, keyFile string) (tls.Certificate, error) {
	certPEMBlock, err := cache.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEMBlock, err := cache.ReadFile(keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
}

// ServeHTTPS is net/http's ListenAndServeTLS, but it reads the key pair from cache instead of local disk.
func ServeHTTPS(addr string, cache *diskcache.Cache, certFile, keyFile string, handler http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: handler}
	addr = srv.Addr
	if addr == "" {
		addr = ":https"
	}
	config := &tls.Config{}
	if srv.TLSConfig != nil {
		*config = *srv.TLSConfig
	}
	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1"}
	}

	var err error
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = LoadX509KeyPair(cache, certFile, keyFile)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	tlsListener := tls.NewListener(tcpKeepAliveListener{ln.(*net.TCPListener)}, config)
	return srv.Serve(tlsListener)
}

// ServeHTTP invokes net/http's ListenAndServe;
// it is here only for symmetry with ServeHTTPS.
func ServeHTTP(addr string, handler http.Handler) error {
	return http.ListenAndServe(addr, handler)
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
//
// Copied from net/http.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
