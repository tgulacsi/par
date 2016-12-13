package main

import (
	"flag"
	"log"
	"os"

	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
)

func main() {
	flagDataShards := flag.Int("d", 10, "data shards")
	flagParityShards := flag.Int("p", 3, "parity shards")

	flag.Parse()
	for _, fn := range flag.Args() {
		if err := ParFile(fn, *flagDataShards, *flagParityShards); err != nil {
			log.Fatal(err)
		}
	}
}

func ParFile(fn string, D, P int) error {
	fh, err := os.Open(fn)
	if err != nil {
		return errors.Wrap(err, fn)
	}
	defer fh.Close()

	enc, err := reedsolomon.New(D, P)
	data := make([][]byte, D, P)
}
