package main

import (
	"archive/tar"
	"encoding/json"
	"hash/crc32"
	"io"
	"log"
	"strings"

	"github.com/pkg/errors"
)

func newTarNextShard(meta FileMetadata, parity *tar.Reader, data io.Reader) func([]byte, int) (ShardMetadata, []byte, error) {
	D := int(meta.DataShards)
	hsh := crc32.New(crc32cTable)
	return func(p []byte, i int) (ShardMetadata, []byte, error) {
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
			return sm, nil, errors.Wrap(err, fn)
		}

		if sm.Size == 0 {
			return sm, p, nil
		}

		r := io.Reader(parity)
		if meta.OnlyParity && i < D {
			r = data
		}
		length := int(sm.Size)
		hsh.Reset()
		n, err := io.ReadFull(io.TeeReader(r, hsh), p[:length])
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
			zero(p[length:])
			hsh.Write(p[length:])
		}
		got := uint32(hsh.Sum32())
		if sm.Hash32 == got {
			return sm, p, nil
		}
		err = errors.Wrapf(errShardBroken, "%d. shard crc mismatch (got %d, wanted %d)!", i, got, sm.Hash32)
		log.Printf("%v", err)
		return sm, nil, err

	}
}
