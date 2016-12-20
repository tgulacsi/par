package main

import (
	"encoding/json"
	"hash/crc32"
	"io"
)

var _ = io.WriteCloser((*rsJSONWriter)(nil))

type rsJSONWriter struct {
	rsEnc
	w     io.Writer
	meta  FileMetadata
	Index uint32
}

func NewRSJSONWriter(w io.Writer, meta FileMetadata) (*rsJSONWriter, error) {
	jsw := rsJSONWriter{w: w}
	jsw.rsEnc = meta.newRSEnc(jsw.writeShards)
	jsw.meta = meta
	if err := jsw.writeHeader(); err != nil {
		return nil, err
	}
	return &jsw, nil
}

func (rw *rsJSONWriter) writeHeader() error {
	return json.NewEncoder(rw.w).Encode(rw.meta)
}

func (rw *rsJSONWriter) Close() error {
	if rw.i == 0 {
		return nil
	}
	err := rw.WriteShards()
	rw.data = nil
	rw.slices = nil
	rw.w = nil
	return err
}

func (rw *rsJSONWriter) writeShards(slices [][]byte, length int) error {
	for i, b := range slices {
		n := len(b)
		isDataShard := i < int(rw.meta.DataShards)
		if isDataShard {
			if n > length {
				n = length
			}
			length -= n
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
