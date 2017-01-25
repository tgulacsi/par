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

	"github.com/pkg/errors"
	"github.com/tgulacsi/par/par2"
)

const Creator = "github.com/tgulacsi/par"

var _ = io.WriteCloser((*rsPAR2Writer)(nil))

type rsPAR2Writer struct {
	rsEnc
	w      io.Writer
	meta   FileMetadata
	FileID par2.MD5
	Header par2.Header
	// raidPkts contains the packets to be repeated
	raidPkts []par2.Packet

	e expCount
}

func NewPAR2Writer(w io.Writer, meta FileMetadata) (*rsPAR2Writer, error) {
	prw := rsPAR2Writer{w: w}
	prw.rsEnc = meta.newRSEnc(prw.writeShards)
	prw.meta = meta
	prw.meta.FileName = filepath.Base(prw.meta.FileName)
	fh, err := os.Open(meta.FileName)
	if err != nil {
		return nil, errors.Wrap(err, meta.FileName)
	}

	mb := par2.NewMainBuilder(int(meta.ShardSize))
	fDescPkt, ifsc, err := mb.AddReader(prw.meta.FileName, fh)
	fh.Close()
	if err != nil {
		return nil, err
	}
	mainPkt := mb.Finish()
	prw.FileID = fDescPkt.FileID

	prw.Header = mainPkt.Header
	prw.raidPkts = []par2.Packet{mainPkt, fDescPkt}

	crPkt := par2.CreatePacket(par2.TypeCreatorPacket).(*par2.CreatorPacket)
	crPkt.RecoverySetID = mainPkt.RecoverySetID
	crPkt.Creator = Creator
	if err := writePackets(w, append(prw.raidPkts, crPkt, ifsc)); err != nil {
		return nil, err
	}
	return &prw, nil
}

func (rw *rsPAR2Writer) Close() error {
	err := rw.WriteShards()
	if err != nil {
		return err
	}
	return writePackets(rw.w, rw.raidPkts)
}

func (rw *rsPAR2Writer) writeShards(slices [][]byte, length int) error {
	h := rw.Header
	h.SetType(par2.TypeRecoverySlicePacket)
	recov := *(h.Create().(*par2.RecoverySlicePacket))

	log.Printf("writeShards(%d, %d) ds=%d", len(slices), length, rw.meta.DataShards)
	for i, b := range slices {
		isDataShard := i < int(rw.meta.DataShards)
		if isDataShard {
			if len(b) > length {
				b = b[:length]
			}
			length -= len(b)
			continue
		}

		recov.Exponent = rw.e.Next()
		// parity shard
		recov.RecoveryData = b
		if err := writePackets(rw.w, append(rw.raidPkts, &recov)); err != nil {
			return err
		}
	}
	return nil
}

func writePackets(w io.Writer, packets []par2.Packet) error {
	for _, p := range packets {
		if _, err := par2.WritePacket(w, p); err != nil {
			return err
		}
	}
	return nil
}

// The first constant is the first power of two that has order 65535.
// The second constant is the next power of two that has order 65535.
// And so on. A power of two has order 65535 if the exponent is not equal to 0 modulus 3, 5, 17, or 257.
// In C code, that would be (n%3 != 0 && n%5 != 0 && n%17 != 0 && n%257 != 0).
// Note - this is the exponent being tested, and not the constant itself. There are 32768 valid constants.
type expCount uint32

// Next returns the next exponent with order 65535.
func (e *expCount) Next() uint32 {
	for {
		(*e)++
		i := *e
		if i%3 != 0 && i%5 != 0 && i%17 != 0 && i%257 != 0 {
			return (1 << i) % 61429
		}
	}
}

// Reset the exponent counter to 0.
func (e *expCount) Reset() {
	*e = 0
}
