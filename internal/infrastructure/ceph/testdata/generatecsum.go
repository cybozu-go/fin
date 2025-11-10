package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cybozu-go/fin/internal/pkg/csumwriter"
)

// This generator only uses diffChecksumChunkSize. Both full.gz and diff.gz are
// initially stored as diff data, and the checksum chunk size used at that time
// is 2 MiB.
// const rawChecksumChunkSize = 64 * 1024 // 64 KiB
const diffChecksumChunkSize = 2 * 1024 * 1024 // 2 MiB

func generate(inPath string, outPath string, chunkSize int) error {
	f, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	of, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer of.Close()

	cw := csumwriter.NewChecksumWriter(io.Discard, of, chunkSize)
	if _, err := io.Copy(cw, gr); err != nil {
		return fmt.Errorf("copy through checksum writer: %w", err)
	}
	if err := cw.Close(); err != nil {
		return fmt.Errorf("close checksum writer: %w", err)
	}
	return nil
}

func main() {
	flag.Parse()
	files := []string{"full.gz", "diff.gz"}
	if args := flag.Args(); len(args) > 0 {
		files = args
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to get cwd:", err)
		os.Exit(1)
	}

	for _, f := range files {
		in := filepath.Join(cwd, f)

		base := strings.TrimSuffix(f, ".gz")
		out := filepath.Join(cwd, base+".csum")
		fmt.Printf("generating %s -> %s\n", in, out)

		if err := generate(in, out, diffChecksumChunkSize); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	}

}
