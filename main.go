package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
)

const (
	CRC32S = version(iota)

	Version             = CRC32S
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

func RestoreParFile(w io.Writer, parFn, fileName string, D, P, shardSize int) error {
	pfh, err := os.Open(parFn)
	if err != nil {
		return errors.Wrap(err, parFn)
	}
	defer pfh.Close()
	r, err := os.Open(fileName)
	rdr, err := NewParReader(pfh, r, D, P, shardSize)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, rdr)
	return err
}

func NewParReader(parity, data io.Reader, D, P, shardSize int) (io.Reader, error) {
	dec := json.NewDecoder(parity)
	var meta FileMetadata
	if err := dec.Decode(&meta); err != nil {
		log.Printf("Read metadata: %v", err)
		meta.DataShards, meta.ParityShards, meta.ShardSize = uint8(D), uint8(P), uint32(shardSize)
	}

	return meta.NewReader(io.MultiReader(dec.Buffered(), parity), data), nil
}

func (meta FileMetadata) NewReader(parity, data io.Reader) io.Reader {
	if meta.DataShards == 0 {
		meta.DataShards = DefaultDataShards
	}
	if meta.ParityShards == 0 {
		meta.ParityShards = DefaultParityShards
	}

	D, P := int(meta.DataShards), int(meta.ParityShards)
	pr, pw := io.Pipe()
	go func() {
		rse := meta.newRSEnc()
		dec := json.NewDecoder(parity)
		for {
			for i := 0; i < D+P; i++ {
				var sm ShardMetadata
				if err := dec.Decode(&sm); err != nil {
					pw.CloseWithError(err)
					return
				}
				if sm.Size == 0 {
					zero(rse.slices[i])
					continue
				}
			}
		}
	}()

	return pr
}

func zero(p []byte) {
	for i := range p {
		p[i] = 0
	}
}

func CreateParFile(out, inp string, D, P, shardSize int) error {
	log.Printf("Create %q for %q.", out, inp)
	if out == inp {
		return errors.New("inp must be differ from out!")
	}
	fh, err := os.Open(inp)
	if err != nil {
		return errors.Wrap(err, inp)
	}
	defer fh.Close()

	pfh, err := os.Create(out)
	if err != nil {
		return errors.Wrap(err, out)
	}
	defer pfh.Close()
	w, err := FileMetadata{
		DataShards: uint8(D), ParityShards: uint8(P),
		ShardSize:  uint32(shardSize),
		FileName:   filepath.Base(fh.Name()),
		OnlyParity: true,
	}.NewWriter(pfh)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, fh); err != nil {
		return errors.Wrap(err, "copy")
	}
	if err := w.Close(); err != nil {
		return errors.Wrap(err, "close")
	}

	return errors.Wrap(pfh.Close(), pfh.Name())
}

var _ = io.WriteCloser((*rsWriter)(nil))

type rsEnc struct {
	onlyPar bool
	enc     reedsolomon.Encoder
	data    []byte
	slices  [][]byte
}

type rsWriter struct {
	rsEnc
	w     io.Writer
	meta  FileMetadata
	i     int
	Index uint32
}

func (meta FileMetadata) NewWriter(w io.Writer) (*rsWriter, error) {
	rw := rsWriter{
		w:     w,
		meta:  meta,
		rsEnc: meta.newRSEnc(),
	}
	if err := json.NewEncoder(w).Encode(rw.meta); err != nil {
		return nil, err
	}
	return &rw, nil
}

func (meta *FileMetadata) newRSEnc() rsEnc {
	if meta.DataShards == 0 {
		meta.DataShards = DefaultDataShards
	}
	if meta.ParityShards == 0 {
		meta.ParityShards = DefaultParityShards
	}
	if meta.ShardSize == 0 {
		meta.ShardSize = DefaultShardSize
	}
	D, P := int(meta.DataShards), int(meta.ParityShards)
	shardSize := int(meta.ShardSize)
	rse := rsEnc{
		onlyPar: meta.OnlyParity,
		data:    make([]byte, (D+P)*shardSize),
		slices:  make([][]byte, D+P),
	}
	var err error
	if rse.enc, err = reedsolomon.New(D, P); err != nil {
		panic(errors.Wrapf(err, "D=%d P=%d", D, P))
	}
	for i := range rse.slices {
		rse.slices[i] = rse.data[i*shardSize : (i+1)*shardSize]
	}
	return rse
}

func (rw *rsWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var written int
	maxData := int(rw.meta.DataShards) * int(rw.meta.ShardSize)
	for len(p) > 0 {
		n, full := maxData-rw.i, true
		if n > len(p) {
			n, full = len(p), false
		}
		copy(rw.data[rw.i:], p[:n])
		rw.i += n
		if !full {
			written += n
			break
		}
		if err := rw.writeShards(); err != nil {
			return written, err
		}
		p = p[n:]
		written += n
	}
	return written, nil
}

func (rw *rsWriter) Close() error {
	if rw.i == 0 {
		return nil
	}
	err := rw.writeShards()
	rw.data = nil
	rw.slices = nil
	rw.w = nil
	return err
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func (rw *rsWriter) writeShards() error {
	maxData := int(rw.meta.DataShards) * int(rw.meta.ShardSize)
	zero(rw.data[rw.i:maxData])
	if err := rw.enc.Encode(rw.slices); err != nil {
		return errors.Wrapf(err, "RS encode %#v", rw.slices)
	}
	for i, b := range rw.slices {
		n := len(b)
		isDataShard := i < int(rw.meta.DataShards)
		if isDataShard {
			if n > rw.i {
				n = rw.i
			}
			rw.i -= n
		}

		hsh := crc32.New(crc32cTable)
		hsh.Write(b[:n])
		rw.Index++
		if err := json.NewEncoder(rw.w).Encode(ShardMetadata{
			Index:  rw.Index,
			Size:   uint32(n),
			Hash32: hsh.Sum32(),
		}); err != nil {
			return err
		}
		if !isDataShard || !rw.onlyPar {
			if _, err := rw.w.Write(b[:n]); err != nil {
				return err
			}
		}
	}
	return nil
}
