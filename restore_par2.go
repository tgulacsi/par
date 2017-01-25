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
	"crypto/md5"
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

	hCRC := crc32.NewIEEE()
	hMD5 := md5.New()
	D, P := int(meta.DataShards), int(meta.ParityShards)

	totalSize := info.Files[0].FileDescPacket.FileLength
	index, dataIndex := -1, -1
	var got par2.ChecksumPair

	return func(p []byte, i int) (ShardMetadata, []byte, error) {
		if totalSize == 0 {
			return ShardMetadata{}, nil, io.EOF
		}
		index++
		hCRC.Reset()
		hMD5.Reset()
		length := int(info.Main.BlockSize)
		sm := ShardMetadata{Index: uint32(index + 1), Size: meta.ShardSize}

		log.Println(i, totalSize)
		if i < D {
			dataIndex++
			if totalSize < uint64(length) {
				length = int(totalSize)
			}
			totalSize -= uint64(length)
			sm.Size = uint32(length)
			if sm.Size == 0 {
				return sm, p, nil
			}
			n, err := io.ReadFull(io.TeeReader(data, io.MultiWriter(hMD5, hCRC)), p[:length])
			if err != nil {
				if sek, ok := data.(io.Seeker); ok {
					if _, seekErr := sek.Seek(int64(len(p)-n), io.SeekCurrent); seekErr != nil {
						return sm, nil, errors.Wrapf(err, "seek: %v", seekErr)
					}
					return sm, nil, errors.Wrap(errShardBroken, "missing slice")
				}
				return sm, nil, err
			}
			log.Println("read", n)

			if length < len(p) {
				zero(p[length:])
				hCRC.Write(p[length:])
				hMD5.Write(p[length:])
			}
			want := info.Files[0].IFSCPacket.Pairs[dataIndex]
			hCRC.Sum(got.CRC32[:0])
			hMD5.Sum(got.MD5[:0])
			if want == got {
				sm.Hash32 = hCRC.Sum32()
				return sm, p, nil
			}
			err = errors.Wrapf(errShardBroken, "%d. shard crc/md5 mismatch (got %s, wanted %s)!", i, got, want)
			log.Printf("%v", err)
			return sm, nil, err
		}
		// parity
		rd := info.RecoveryData[(index%(D+P))-D]
		if rd.Damaged {
			return sm, nil, errors.Wrap(errShardBroken, strconv.Itoa(i+1))
		}
		hCRC.Write(rd.RecoveryData)
		sm.Hash32 = hCRC.Sum32()
		return sm, rd.RecoveryData, nil
	}
}
