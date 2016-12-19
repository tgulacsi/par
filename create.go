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
	"github.com/tgulacsi/par/par2"
)

const Creator = "github.com/tgulacsi/par"

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
		Version:    DefaultVersion,
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

var _ = io.WriteCloser((*rsJSONWriter)(nil))

type rsEnc struct {
	enc    reedsolomon.Encoder
	data   []byte
	slices [][]byte
}

type rsJSONWriter struct {
	rsEnc
	w     io.Writer
	meta  FileMetadata
	i     int
	Index uint32
}

var _ = io.WriteCloser((*rsPAR2Writer)(nil))

type rsPAR2Writer struct {
	rsEnc
	w       io.Writer
	meta    FileMetadata
	mainPkt *par2.MainPacket
}

func (meta FileMetadata) NewWriter(w io.Writer) (io.WriteCloser, error) {
	var rw io.WriteCloser
	switch meta.Version {
	case VersionJSON:
		jsw := rsJSONWriter{
			w:     w,
			meta:  meta,
			rsEnc: meta.newRSEnc(),
		}
		if err := jsw.writeHeader(); err != nil {
			return nil, err
		}
		rw = &jsw
	case VersionPAR2:
		prw := rsPAR2Writer{
			w: w, meta: meta, rsEnc: meta.newRSEnc(),
			mainPkt: par2.CreatePacket(par2.TypeMainPacket).(*par2.MainPacket)}
		fh, err := os.Open(meta.FileName)
		if err != nil {
			return nil, errors.Wrap(err, meta.FileName)
		}
		fDescPkt, err := prw.mainPkt.Add(fh, meta.FileName)
		fh.Close()
		if err != nil {
			return nil, err
		}
		rw = &prw
	default:
		return nil, errors.Wrapf(ErrUnknownVersion, "%s", meta.Version)
	}

	return rw, nil
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
		data:   make([]byte, (D+P)*shardSize),
		slices: make([][]byte, D+P),
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

func (rw *rsJSONWriter) writeHeader() error {
	return json.NewEncoder(rw.w).Encode(rw.meta)
}

func (rw *rsJSONWriter) Write(p []byte) (int, error) {
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

func (rw *rsJSONWriter) Close() error {
	if rw.i == 0 {
		return nil
	}
	err := rw.writeShards()
	rw.data = nil
	rw.slices = nil
	rw.w = nil
	return err
}

func (rw *rsJSONWriter) writeShards() error {
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
		if !isDataShard || !rw.meta.OnlyParity {
			if _, err := rw.w.Write(b[:n]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rw *rsPAR2Writer) Close() error {
	crPkt := par2.CreatePacket(par2.TypeCreatorPacket).(*par2.CreatorPacket)
	crPkt.Creator = Creator
	_, err := par2.WritePacket(rw.w, crPkt)
	return err
}

func (rw *rsPAR2Writer) Write(p []byte) (int, error) {
}
