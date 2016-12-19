package main

import (
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strings"
)

const (
	VersionJSON = version(iota)
	VersionPAR2

	Version             = VersionPAR2
	DefaultShardSize    = 512 << 10
	DefaultDataShards   = 10
	DefaultParityShards = 3
)

type version uint8

// Need to save the metadata of:
//   1. file (real data) size
//   2. number of data/parity shards
//   3. hash of each shard (to know which shard has to be reconstructeed
//   4. order of the shards
//
type FileMetadata struct {
	Version      version `json:"V"`
	DataShards   uint8   `json:"DS"`
	ParityShards uint8   `json:"PS"`
	ShardSize    uint32  `json:"S"`
	FileName     string  `json:"F"`
	OnlyParity   bool    `json:"OP"`
}
type ShardMetadata struct {
	Index  uint32 `json:"i"`
	Size   uint32 `json:"s"`
	Hash32 uint32 `json:"h"`
}

func main() {
	todo := "create"
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		todo, os.Args[1] = os.Args[1], os.Args[0]
		os.Args = os.Args[1:]
	}
	var dataShards, parityShards, shardSize int
	createFlags := flag.NewFlagSet("create", flag.ExitOnError)
	createFlags.IntVar(&dataShards, "d", DefaultDataShards, "data shards")
	createFlags.IntVar(&parityShards, "p", DefaultParityShards, "parity shards")
	createFlags.IntVar(&shardSize, "s", DefaultShardSize, "shard size")

	restoreFlags := flag.NewFlagSet("restore", flag.ExitOnError)
	restoreFlags.IntVar(&dataShards, "d", 0, "data shards")
	restoreFlags.IntVar(&parityShards, "p", 0, "parity shards")
	restoreFlags.IntVar(&shardSize, "s", 0, "shard size")
	flagOut := restoreFlags.String("o", "-", "output")

	var flagSet *flag.FlagSet
	switch todo {
	case "c", "create":
		todo, flagSet = "create", createFlags
	case "r", "restore":
		todo, flagSet = "restore", restoreFlags
	default:
		fmt.Fprintf(os.Stderr, `Create the parity file:

	par create [options] <file> [file.par]
`)
		createFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Restore the file from the parity:

	par restore <file.par> [file]
`)
		restoreFlags.PrintDefaults()
		os.Exit(1)
	}

	flagSet.Parse(os.Args[1:])
	if todo == "create" {
		inp := flagSet.Arg(0)
		out := inp + ".par"
		if len(flagSet.Args()) > 1 {
			out = flagSet.Arg(1)
		}
		if err := CreateParFile(out, inp, dataShards, parityShards, shardSize); err != nil {
			log.Fatal(err)
		}
		return
	}
	parFn := flagSet.Arg(0)
	fileName := strings.TrimSuffix(parFn, ".par")
	if len(flagSet.Args()) > 1 {
		fileName = flagSet.Arg(1)
	}
	w := io.WriteCloser(os.Stdout)
	if !(*flagOut == "" || *flagOut == "-") {
		var err error
		if w, err = os.Create(*flagOut); err != nil {
			log.Fatal(err)
		}
		defer w.Close()
	}
	if err := RestoreParFile(w, parFn, fileName, dataShards, parityShards, shardSize); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
}

func zero(p []byte) {
	for i := range p {
		p[i] = 0
	}
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type errReader struct{ err error }

func (r errReader) Read(_ []byte) (int, error) { return 0, r.err }
