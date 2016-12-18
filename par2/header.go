// Copyright (c) 2016 Tamás Gulácsi
// Copyright (c) 2013 Michael Tighe
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
	"bytes"
	"encoding/binary"
	"io"
)

const validSequence string = "PAR2\000PKT"

type Header struct {
	Sequence      [8]byte
	Length        uint64
	PacketMD5     MD5
	RecoverySetID MD5
	Type          [16]byte
	Damaged       bool
}

func (h *Header) readFrom(r io.Reader) error {
	_, err := io.ReadFull(r, h.Sequence[:])
	if err != nil {
		return err
	}

	binary.Read(r, binary.LittleEndian, &h.Length)
	io.ReadFull(r, h.PacketMD5[:])
	io.ReadFull(r, h.RecoverySetID[:])
	_, err = io.ReadFull(r, h.Type[:])
	return err
}

func (h *Header) ValidSequence() bool {
	return bytes.Equal(h.Sequence[:], []byte(validSequence))
}

func (h *Header) writeTo(w io.Writer) error {
	_, err := w.Write(h.Sequence[:])
	if err != nil {
		return err
	}
	binary.Write(w, binary.LittleEndian, h.Length)
	w.Write(h.PacketMD5[:])
	w.Write(h.RecoverySetID[:])
	_, err = w.Write(h.Type[:])
	return err
}
