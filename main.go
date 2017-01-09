// Copyright 2016 Tamás Gulácsi
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/tgulacsi/par/par2"
)

const (
	VersionJSON = version(iota)
	VersionPAR2
	VersionTAR

	DefaultVersion      = VersionTAR
	DefaultShardSize    = 512 << 10
	DefaultDataShards   = 10
	DefaultParityShards = 3
)

var ErrUnknownVersion = errors.New("unknown version")

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
	var redundancy, shardSize int
	var verS string
	createFlags := flag.NewFlagSet("create", flag.ExitOnError)
	createFlags.IntVar(&redundancy, "r", 30, "data shards")
	createFlags.IntVar(&shardSize, "s", DefaultShardSize, "shard size")
	createFlags.StringVar(&verS, "version", "tar", "version to create (tar|json|par2)")

	restoreFlags := flag.NewFlagSet("restore", flag.ExitOnError)
	flagOut := restoreFlags.String("o", "-", "output")

	dumpFlags := flag.NewFlagSet("dump", flag.ExitOnError)

	var flagSet *flag.FlagSet
	switch todo {
	case "c", "create":
		todo, flagSet = "create", createFlags
	case "r", "restore":
		todo, flagSet = "restore", restoreFlags
	case "d", "dump":
		todo, flagSet = "dump", dumpFlags
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
		fmt.Fprintf(os.Stderr, `

Dump the file's contents for debugging:

	par dump <file.par>
`)
		dumpFlags.PrintDefaults()

		os.Exit(1)
	}

	flagSet.Parse(os.Args[1:])
	switch todo {
	case "create":
		inp := flagSet.Arg(0)
		out := inp + ".par"
		if len(flagSet.Args()) > 1 {
			out = flagSet.Arg(1)
		}
		ver := VersionTAR
		switch verS {
		case "json":
			ver = VersionJSON
		case "par", "par2":
			ver = VersionPAR2
		case "tar":
			ver = VersionTAR
		default:
			fmt.Fprintf(os.Stderr, "Unknown version %q. Known versions: json, tar, par2.")
			os.Exit(1)
		}
		var dataShards, parityShards int
		if redundancy%10 == 0 {
			dataShards, parityShards = 10, redundancy/10
		} else {
			dataShards, parityShards = 100, redundancy
		}
		if err := ver.CreateParFile(out, inp, dataShards, parityShards, shardSize); err != nil {
			log.Fatal(err)
		}
		return
	case "dump":
		files := flagSet.Args()
		fh, err := os.Open(files[0])
		var a [1024]byte
		n, err := io.ReadAtLeast(fh, a[:], 512)
		if err != nil && n < 8 {
			log.Fatal(err)
		}
		b := a[:n]
		ver := VersionTAR
		if len(b) > 256 && bytes.Contains(b[256:], []byte("\000ustar  \000")) {
			ver = VersionTAR
		} else if len(b) > 1 && b[0] == '{' {
			ver = VersionJSON
		} else {
			ver = VersionPAR2
		}

		switch ver {
		case VersionPAR2:
			stat := &par2.ParInfo{
				ParFiles: files,
			}
			if err := stat.Parse(); err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			enc.Encode(stat)
		default:
			log.Fatal(errors.Errorf("dumping version %s not implemented", ver))
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
	if err := RestoreParFile(w, parFn, fileName); err != nil {
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
