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
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"strings"

	"github.com/pkg/errors"
)

func newTarNextShard(meta FileMetadata, parity *tar.Reader, data io.Reader) func([]byte, int) (ShardMetadata, []byte, error) {
	if meta.Version != VersionTAR {
		panic(fmt.Sprintf("Version mismatch: got %s, wanted %s", meta.Version, VersionTAR))
	}
	D := int(meta.DataShards)
	hsh := crc32.New(crc32cTable)
	return func(p []byte, idx int) (ShardMetadata, []byte, error) {
		var fn string
		var sm ShardMetadata
		for {
			th, err := parity.Next()
			if err != nil {
				return sm, nil, err
			}
			if i := strings.IndexByte(th.Name, '{'); i >= 0 {
				fn = th.Name[i:]
				break
			}
		}
		if err := json.NewDecoder(strings.NewReader(fn)).Decode(&sm); err != nil {
			log.Printf("decode %q: %v", fn, err)
			return sm, nil, errors.Wrap(err, fn)
		}

		if sm.Size == 0 {
			return sm, p, nil
		}

		r, source := io.Reader(parity), "parity"
		log.Printf("onlyPar=%t idx=%d D=%d", meta.OnlyParity, idx, D)
		if meta.OnlyParity && idx < D {
			r, source = data, "data"
		}
		length := int(sm.Size)
		hsh.Reset()
		n, err := io.ReadFull(io.TeeReader(r, hsh), p[:length])
		log.Printf("Read %d bytes from %s: %v", n, source, err)
		if err != nil {
			if sek, ok := r.(io.Seeker); ok {
				if _, seekErr := sek.Seek(int64(len(p)-n), io.SeekCurrent); seekErr != nil {
					return sm, nil, errors.Wrapf(err, "seek: %v", seekErr)
				}
				return sm, nil, errors.Wrap(errShardBroken, "missing slice")
			}
			return sm, nil, err
		}

		if length < len(p) {
			zero(p[length:cap(p)])
		}
		got := uint32(hsh.Sum32())
		if sm.Hash32 == got {
			return sm, p, nil
		}
		err = errors.Wrapf(errShardBroken, "%d. shard crc mismatch (got %d, wanted %d)!", idx, got, sm.Hash32)
		log.Printf("%v", err)
		return sm, nil, err

	}
}
