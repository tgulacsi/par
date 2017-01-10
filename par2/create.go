// Copyright (c) 2016 Tamás Gulácsi
//
// The MIT License (MIT)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package par2

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/tgulacsi/par/par2"
)

type parWriter struct {
	Main MainPacket
}

func NewWriter() *parWriter {
	return &parWriter{Main: CreatePacket(TypeMainPacket).(*MainPacket)}
}

func (pw *parWriter) AddFile(name string) error {
	fh, err := os.Open(name)
	if err != nil {
		return errors.Wrap(err, name)
	}
	err = pw.AddReader(name, fh)
	_ = fh.Close()
	return err
}

// AddReader adds the reader with the given filename to the recovery set.
func (pw *parWriter) AddReader(name string, r io.Reader) error {
	fDescPkt, err := pw.Main.Add(r, name)
	if err != nil {
		return err
	}
	prw.FileID = fDescPkt.FileID

	prw.Header = mainPkt.Header
	prw.raidPkts = []par2.Packet{mainPkt, fDescPkt}

	crPkt := par2.CreatePacket(par2.TypeCreatorPacket).(*par2.CreatorPacket)
	crPkt.RecoverySetID = mainPkt.RecoverySetID
	crPkt.Creator = Creator
	if err := writePackets(w, append(prw.raidPkts, crPkt, mainPkt, fDescPkt)); err != nil {
		return nil, err
	}
	return &prw, nil
}

func (rw *rsPAR2Writer) Close() error {
	err := rw.WriteShards()
	if err != nil {
		return err
	}
	return writePackets(rw.w,
		append(append([]par2.Packet{rw.ifsc}, rw.raidPkts...), rw.ifsc))
}
