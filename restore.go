package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/pkg/errors"
)

func RestoreParFile(w io.Writer, parFn, fileName string, D, P, shardSize int) error {
	pfh, err := os.Open(parFn)
	if err != nil {
		return errors.Wrap(err, parFn)
	}
	defer pfh.Close()
	br := bufio.NewReader(pfh)
	b, err := br.Peek(5)
	if err != nil {
		return errors.Wrap(err, parFn)
	}
	ver := VersionPAR2
	if !bytes.Equal(b, []byte("PAR2\000")) {
		if b[0] == '{' {
			ver = VersionJSON
		} else {
			return errors.Errorf("unknown parity file start %q", b)
		}
	}
	r, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, fileName)
	}
	wr, err := ver.NewParWriterTo(br, r, D, P, shardSize)
	if err != nil {
		return err
	}
	_, err = wr.WriteTo(w)
	return err
}

func (ver version) NewParWriterTo(parity, data io.Reader, D, P, shardSize int) (io.WriterTo, error) {
	var meta FileMetadata
	switch ver {
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

func (meta FileMetadata) NewWriterTo(parity, data io.Reader) io.WriterTo {
	if meta.DataShards == 0 {
		meta.DataShards = DefaultDataShards
	}
	if meta.ParityShards == 0 {
		meta.ParityShards = DefaultParityShards
	}

	rsw := rsWriterTo{
		meta:   meta,
		parity: bufio.NewReader(parity),
		data:   data,
	}
	rsw.rsEnc = meta.newRSEnc(rsw.writeShards)
	return &rsw
}

var _ = io.WriterTo(rsWriterTo{})

type rsWriterTo struct {
	rsEnc
	parity *bufio.Reader
	data   io.Reader
	meta   FileMetadata
}

func (rsw rsWriterTo) WriteTo(w io.Writer) (int64, error) {
	D, P := int(rsw.meta.DataShards), int(rsw.meta.ParityShards)
	if (rsw.data == nil || rsw.parity == rsw.data) && rsw.meta.OnlyParity {
		return 0, errors.New("OnlyPar needs separate data file")
	}

	slices := make([][]byte, len(rsw.slices))
	var index uint32
	var written int64
	for {
		copy(slices, rsw.slices)
		var missing, totalSize int
		for i := 0; i < D+P; i++ {
			index++
			var sm ShardMetadata
			b, err := rsw.parity.ReadBytes('\n')
			if err != nil {
				if err == io.EOF && len(b) == 0 {
					return written, nil
				}
				return written, nil
			}
			if err := json.Unmarshal(b, &sm); err != nil {
				return written, err
			}
			if sm.Index != index {
				return written, errors.Errorf("Index mismatch: got %d, wanted %d.", sm.Index, index)
			}
			if sm.Size == 0 {
				zero(slices[i])
				continue
			}
			r := io.Reader(rsw.parity)
			if rsw.meta.OnlyParity && i < D {
				r = rsw.data
			}
			hsh := crc32.New(crc32cTable)
			length := int(sm.Size)
			n, err := io.ReadFull(io.TeeReader(r, hsh), slices[i][:length])
			if i < D {
				totalSize += length
			}
			if err == nil {
				if length < len(slices[i]) {
					zero(slices[i][length:])
					hsh.Write(slices[i][length:])
				}
				got := uint32(hsh.Sum32())
				if sm.Hash32 != got {
					log.Printf("%d. shard crc mismatch (got %d, wanted %d)!", i, got, sm.Hash32)
					slices[i] = nil
					missing++
				}
				continue
			}
			if sek, ok := r.(io.Seeker); ok {
				if _, seekErr := sek.Seek(int64(len(slices[i])-n), io.SeekCurrent); seekErr != nil {
					return written, errors.Wrapf(err, "seek: %v", seekErr)
				}
				slices[i] = nil // missing slice!
				missing++
			}

		}

		if missing > 0 {
			log.Printf("Has %d missing shards, try to reconstruct...", missing)
			if err := rsw.enc.Reconstruct(slices); err != nil {
				return written, errors.Wrap(err, "Reconstruct")
			}
		}
		if ok, err := rsw.enc.Verify(slices); !ok || err != nil {
			if err == nil {
				err = errors.New("Verify failed")
			}
			return written, errors.Wrap(err, "Verify")
		}

		if _, err := w.Write(rsw.rsEnc.data[:totalSize]); err != nil {
			return written, err
		}
	}
	return written, nil
}
