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
	"bytes"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

var _ = io.WriteCloser((*rsTarWriter)(nil))

type rsTarWriter struct {
	rsEnc
	w     *tar.Writer
	meta  FileMetadata
	Index uint32
}

func NewRSTarWriter(w io.Writer, meta FileMetadata) (*rsTarWriter, error) {
	tw := rsTarWriter{w: tar.NewWriter(w)}
	tw.rsEnc = meta.newRSEnc(tw.writeShards)
	tw.meta = meta
	meta.FileName = filepath.Base(meta.FileName)
	b, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	return &tw, tw.add("FileMetadata.json", b)
}

var (
	uid = os.Getuid()
	gid = os.Getgid()
	now = time.Now()
)

func (rw *rsTarWriter) add(name string, data []byte) error {
	if err := rw.w.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0444, Uid: uid, Gid: gid, Size: int64(len(data)),
		ModTime: now,
	}); err != nil {
		return errors.Wrap(err, name)
	}
	_, err := rw.w.Write(data)
	return err
}

func (rw *rsTarWriter) Close() error {
	if rw.w == nil {
		return nil
	}
	if rw.i == 0 {
		return rw.w.Close()
	}
	err := rw.WriteShards()
	if closeErr := rw.w.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	rw.data = nil
	rw.slices = nil
	rw.w = nil
	return err
}

func (rw *rsTarWriter) writeShards(slices [][]byte, length int) error {
	var buf bytes.Buffer
	for i, b := range slices {
		n := len(b)
		isDataShard := i < int(rw.meta.DataShards)
		if isDataShard {
			if n > length {
				n = length
			}
			length -= n
		}
		b = b[:n]

		hsh := crc32.New(crc32cTable)
		hsh.Write(b[:n])
		rw.Index++

		sm := ShardMetadata{
			Index:  rw.Index,
			Size:   uint32(n),
			Hash32: hsh.Sum32(),
		}
		buf.Reset()
		buf.WriteString("shard-")
		if err := json.NewEncoder(&buf).Encode(sm); err != nil {
			return errors.Wrapf(err, "marshal %#v", sm)
		}
		fn := string(append(bytes.TrimSpace(buf.Bytes()), []byte(".dat")...))
		if isDataShard && rw.meta.OnlyParity {
			b = nil
		}
		if err := rw.add(fn, b); err != nil {
			return err
		}
	}
	return nil
}
