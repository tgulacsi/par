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
	"hash/crc32"
	"io"
	"log"
	"strconv"

	"github.com/pkg/errors"
	"github.com/tgulacsi/par/par2"
)

var errRecoveryDataInvalid = errors.New("recovery data is invalid")

func newPAR2NextShard(meta FileMetadata, parity namedReader, data io.Reader) func([]byte, int) (ShardMetadata, []byte, error) {
	info := par2.ParInfo{ParFiles: []string{parity.Name()}}
	var err error
	if err = info.Parse(); err == nil {
		if info.Main == nil {
			err = errors.New("empty par file: " + parity.Name())
		}
	}
	if err != nil {
		return func(_ []byte, _ int) (ShardMetadata, []byte, error) { return ShardMetadata{}, nil, err }
	}

	hsh := crc32.New(crc32cTable)
	D := int(meta.DataShards)

	// FIXME(tgulacsi): data shards, then parity shards

	return func(p []byte, i int) (ShardMetadata, []byte, error) {
		log.Printf("i=%d", i)
		rd := info.RecoveryData[i]
		if rd.Damaged {
			return ShardMetadata{}, nil, errors.Wrap(errRecoveryDataInvalid, strconv.Itoa(i+1))
		}
		hsh.Reset()
		hsh.Write(rd.RecoveryData)
		sm := ShardMetadata{Index: uint32(i + 1), Size: uint32(info.TotalSize), Hash32: hsh.Sum32()}
		return sm, rd.RecoveryData, nil
	}
}
