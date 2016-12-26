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
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
	"github.com/tgulacsi/par/par2"
)

var errShardBroken = errors.New("shard is broken")

func RestoreParFile(w io.Writer, parFn, fileName string, D, P, shardSize int) error {
	pfh, err := os.Open(parFn)
	if err != nil {
		return errors.Wrap(err, parFn)
	}
	defer pfh.Close()
	br := bufio.NewReader(pfh)
	// u s t a r \0 0 0  at byte offset 257
	b, err := br.Peek(257 + 6)
	if err != nil {
		return errors.Wrap(err, parFn)
	}
	ver := VersionTAR
	if bytes.Equal(b, []byte("PAR2\000")) {
		ver = VersionPAR2
	} else if b[0] == '{' {
		ver = VersionJSON
	} else if len(b) >= 257 && bytes.Equal(b[257:257+6], []byte("ustar\000")) {
		ver = VersionTAR
	} else {
		return errors.Errorf("unknown parity file start %q", b)
	}

	if ver == VersionPAR2 {
		info := par2.ParInfo{ParFiles: []string{parFn}}
		err := info.Parse()
		log.Printf("info=%q err=%+v", info, err)
		if err == nil && info.Main == nil {
			err = errors.New("empty par file: " + parFn)
		}
		return err
	}

	r, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, fileName)
	}
	wr, err := ver.NewParWriterTo(br, r, D, P, shardSize)
	if err != nil {
		return err
	}
	n, err := wr.WriteTo(w)
	log.Printf("Written %d bytes.", n)
	return err
}

func (ver version) NewParWriterTo(parity, data io.Reader, D, P, shardSize int) (io.WriterTo, error) {
	var meta FileMetadata
	switch ver {
	case VersionTAR:
		tr := tar.NewReader(parity)
		th, err := tr.Next()
		if err != nil {
			return nil, err
		}
		if th.Name != "FileMetadata.json" {
			return nil, errors.Errorf("First item should be FileMetadata.json, got %q", th.Name)
		}
		meta.Version = VersionTAR
		return meta.NewWriterTo(tr, data), nil

	case VersionJSON:
		dec := json.NewDecoder(parity)
		if err := dec.Decode(&meta); err != nil {
			log.Printf("Read metadata: %v", err)
			meta.DataShards, meta.ParityShards, meta.ShardSize = uint8(D), uint8(P), uint32(shardSize)
		}
		meta.Version = VersionJSON
		return meta.NewWriterTo(rewind(dec.Buffered(), parity), data), nil
	case VersionPAR2:
		meta.Version = VersionPAR2
		return meta.NewWriterTo(parity, data), nil

	}
	return nil, errors.Errorf("unknown version %s", ver)
}

func rewind(ahead, rest io.Reader) io.Reader {
	sek, ok := rest.(io.Seeker)
	if !ok {
		return io.MultiReader(ahead, rest)
	}
	b, err := ioutil.ReadAll(ahead)
	if err != nil {
		return errReader{err}
	}
	n := len(b)
	if !bytes.HasSuffix(b, []byte{'\n'}) {
		n--
	}
	if _, err := sek.Seek(int64(-n), io.SeekCurrent); err != nil {
		return errReader{err}
	}
	return rest
}

func (meta *FileMetadata) NewWriterTo(parity, data io.Reader) io.WriterTo {
	if meta.DataShards == 0 {
		meta.DataShards = DefaultDataShards
	}
	if meta.ParityShards == 0 {
		meta.ParityShards = DefaultParityShards
	}
	if n := meta.ShardSize % 4; n != 0 && meta.Version == VersionPAR2 {
		meta.ShardSize += 4 - n
	}

	rsw := rsWriterTo{meta: meta}

	var nextShard func([]byte, int) (ShardMetadata, []byte, error)
	switch meta.Version {
	case VersionJSON:
		nextShard = newJSONNextShard(*meta, bufio.NewReader(parity), data)

	case VersionTAR:
		nextShard = newTarNextShard(*meta, parity.(*tar.Reader), data)

	case VersionPAR2:
		panic("PAR2 decoding is not implemented")

	default:
		panic(fmt.Sprintf("Unknown version %v", meta.Version))
	}

	rsw.rsDec = meta.newRSDec(nextShard)
	return &rsw
}

func (meta *FileMetadata) newRSDec(nextShard func([]byte, int) (ShardMetadata, []byte, error)) rsDec {
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
	rse := rsDec{
		data:       make([]byte, (D+P)*shardSize),
		slices:     make([][]byte, D+P),
		DataShards: D, ShardSize: shardSize,
		nextShard: nextShard,
	}
	var err error
	if rse.Encoder, err = reedsolomon.New(D, P); err != nil {
		panic(errors.Wrapf(err, "D=%d P=%d", D, P))
	}
	for i := range rse.slices {
		rse.slices[i] = rse.data[i*shardSize : (i+1)*shardSize]
	}
	return rse
}

var _ = io.WriterTo(rsWriterTo{})

type rsWriterTo struct {
	meta *FileMetadata
	rsDec
}

type rsDec struct {
	reedsolomon.Encoder
	data                  []byte
	slices                [][]byte
	DataShards, ShardSize int
	nextShard             func([]byte, int) (ShardMetadata, []byte, error)
}

func (rsw rsWriterTo) WriteTo(w io.Writer) (int64, error) {
	D, P := int(rsw.meta.DataShards), int(rsw.meta.ParityShards)
	slices := make([][]byte, len(rsw.slices))
	var (
		index   uint32
		written int64
		sm      ShardMetadata
		err     error
	)
	for {
		copy(slices, rsw.slices)
		var missing, totalSize int
		for i := 0; i < D+P; i++ {
			p := slices[i]
			p = p[:cap(p)]
			index++
			sm, p, err = rsw.nextShard(p, i)
			if err != nil {
				if err == io.EOF {
					return written, nil
				}
				if errors.Cause(err) == errShardBroken {
					slices[i] = nil
					missing++
					continue
				}
				return written, err
			}

			if sm.Index != index {
				return written, errors.Errorf("Index mismatch: got %d, wanted %d.", sm.Index, index)
			}
			length := int(sm.Size)
			if length == 0 {
				zero(p[:cap(p)])
				continue
			}
			if i < D {
				totalSize += length
			}
			zero(p[length:cap(p)])
		}

		if missing > 0 {
			log.Printf("Has %d missing shards, try to reconstruct...", missing)
			if err := rsw.rsDec.Reconstruct(slices); err != nil {
				return written, errors.Wrap(err, "Reconstruct")
			}
		}
		if ok, err := rsw.rsDec.Verify(slices); !ok || err != nil {
			if err == nil {
				err = errors.New("Verify failed")
			}
			return written, errors.Wrap(err, "Verify")
		}

		n, err := w.Write(rsw.rsDec.data[:totalSize])
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}
