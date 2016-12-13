package main

import (
	"encoding/json"
	"flag"
	"hash/crc32"
	"io"
	"log"
	"os"

	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
)

const (
	CRC32S = version(iota)

	Version   = CRC32S
	ShardSize = 4 << 10
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
}
type ShardMetadata struct {
	Index  uint32 `json:"i"`
	Size   uint32 `json:"s"`
	Hash32 uint32 `json:"h"`
}

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

	pfh, err := os.Create(fn + ".par")
	if err != nil {
		return errors.Wrap(err, fn+".par")
	}
	defer pfh.Close()
	w, err := NewWriter(pfh, D, P)
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

type rsWriter struct {
	w      io.Writer
	meta   FileMetadata
	enc    reedsolomon.Encoder
	data   []byte
	slices [][]byte
	i      int
	Index  uint32
}

func NewWriter(w io.Writer, D, P int) (*rsWriter, error) {
	rw := rsWriter{
		w:      w,
		data:   make([]byte, (D+P)*ShardSize),
		slices: make([][]byte, D+P),
		meta: FileMetadata{
			Version:    Version,
			ShardSize:  ShardSize,
			DataShards: uint8(D), ParityShards: uint8(P),
		},
	}
	var err error
	if rw.enc, err = reedsolomon.New(D, P); err != nil {
		return nil, err
	}
	if err = json.NewEncoder(w).Encode(rw.meta); err != nil {
		return nil, err
	}
	for i := range rw.slices {
		rw.slices[i] = rw.data[i*ShardSize : (i+1)*ShardSize]
	}
	return &rw, nil
}

func (rw *rsWriter) Write(p []byte) (int, error) {
	var written int
	maxData := int(rw.meta.DataShards) * int(rw.meta.ShardSize)
	for len(p) > 0 {
		n, full := maxData-rw.i, true
		if n > len(p) {
			n, full = len(p), false
		}
		copy(rw.data[rw.i:], p[:n])
		if !full {
			written += n
			rw.i += n
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
	for i := rw.i; i < maxData; i++ {
		rw.data[i] = 0
	}
	if err := rw.enc.Encode(rw.slices); err != nil {
		return err
	}
	for _, b := range rw.slices {
		n := len(b)
		if n > rw.i {
			n = rw.i
		}
		rw.i -= n

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
		if _, err := rw.w.Write(b[:n]); err != nil {
			return err
		}
	}
	return nil
}
