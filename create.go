package main

import (
	"encoding/json"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
)

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