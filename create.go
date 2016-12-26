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
	if n := shardSize % 4; n != 0 {
		shardSize += 4 - n
	}
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

type rsEnc struct {
	enc                   reedsolomon.Encoder
	data                  []byte
	slices                [][]byte
	DataShards, ShardSize int
	i                     int
	writeShards           func([][]byte, int) error
}

func (meta FileMetadata) NewWriter(w io.Writer) (io.WriteCloser, error) {
	var rw io.WriteCloser
	var err error

	switch meta.Version {
	case VersionJSON:
		rw, err = NewRSJSONWriter(w, meta)
	case VersionPAR2:
		rw, err = NewPAR2Writer(w, meta)
	case VersionTAR:
		rw, err = NewRSTarWriter(w, meta)
	default:
		err = errors.Wrapf(ErrUnknownVersion, "%s", meta.Version)
	}

	return rw, err
}

func (meta *FileMetadata) newRSEnc(writeShards func([][]byte, int) error) rsEnc {
	if meta.DataShards == 0 {
		meta.DataShards = DefaultDataShards
	}
	if meta.ParityShards == 0 {
		meta.ParityShards = DefaultParityShards
	}
	if meta.ShardSize == 0 {
		meta.ShardSize = DefaultShardSize
	}
	if n := meta.ShardSize % 4; n != 0 {
		n += 4 - n
	}
	D, P := int(meta.DataShards), int(meta.ParityShards)
	shardSize := int(meta.ShardSize)
	rse := rsEnc{
		data:        make([]byte, (D+P)*shardSize),
		slices:      make([][]byte, D+P),
		writeShards: writeShards,
		DataShards:  D, ShardSize: shardSize,
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

func (rse *rsEnc) Write(p []byte) (int, error) {
	log.Printf("Write(%d)", len(p))
	if len(p) == 0 {
		return 0, nil
	}
	var written int
	maxData := rse.DataShards * rse.ShardSize
	for len(p) > 0 {
		n, full := maxData-rse.i, true
		if n > len(p) {
			n, full = len(p), false
		}
		copy(rse.data[rse.i:], p[:n])
		rse.i += n
		if !full {
			written += n
			break
		}

		// full, write out shards
		if err := rse.WriteShards(); err != nil {
			return written, err
		}
		p = p[n:]
		written += n
	}
	return written, nil
}

func (rse *rsEnc) WriteShards() error {
	maxData := rse.DataShards * rse.ShardSize
	zero(rse.data[rse.i:maxData])
	if err := rse.enc.Encode(rse.slices); err != nil {
		return errors.Wrapf(err, "RS encode %#v", rse.slices)
	}
	if err := rse.writeShards(rse.slices, rse.i); err != nil {
		return err
	}
	rse.i = 0
	return nil
}
