package main

import (
	"fmt"

	"github.com/amber-store/core/chunkers"
	"github.com/amber-store/core/ingest"
	"github.com/urfave/cli/v2"
)

// chunkConfig holds the content-defined-chunking parameters used by the ingest
// command when building the tree.
type chunkConfig struct {
	min            int
	avg            int
	max            int
	itemBits       int
	xattrInlineMax int
}

// chunkOpts maps the CLI chunking flags onto the library options. min/avg/max
// must all be set together or all left unset (the library defaults).
func (cc *chunkConfig) chunkOpts() (ingest.ChunkOpts, error) {
	opts := ingest.ChunkOpts{ItemBits: cc.itemBits, XattrInlineMax: cc.xattrInlineMax}
	if cc.min == 0 && cc.avg == 0 && cc.max == 0 {
		return opts, nil
	}
	if cc.min <= 0 || cc.avg <= 0 || cc.max <= 0 {
		return ingest.ChunkOpts{}, fmt.Errorf("--min, --avg and --max must all be set together")
	}
	opts.Byte = &chunkers.ByteOpts{MinSize: cc.min, NormalSize: cc.avg, MaxSize: cc.max}
	return opts, nil
}

// chunkFlags returns the CLI flags that fill cc.
func chunkFlags(cc *chunkConfig) []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:        "min",
			Usage:       "ultracdc minimum chunk size in bytes",
			Destination: &cc.min,
			Value:       32 << 10,
		},
		&cli.IntFlag{
			Name:        "avg",
			Usage:       "ultracdc average (normal) chunk size in bytes",
			Destination: &cc.avg,
			Value:       128 << 10,
		},
		&cli.IntFlag{
			Name:        "max",
			Usage:       "ultracdc maximum chunk size in bytes",
			Destination: &cc.max,
			Value:       256 << 10,
		},
		&cli.IntFlag{
			Name:        "item-bits",
			Value:       ingest.DefaultItemBits,
			Usage:       "item chunker average run = 2^bits",
			Destination: &cc.itemBits,
		},
		&cli.IntFlag{
			Name:        "xattr-inline-max",
			Value:       ingest.DefaultXattrInlineMax,
			Usage:       "xattrs larger than this many bytes spill to an XattrSet",
			Destination: &cc.xattrInlineMax,
		},
	}
}
