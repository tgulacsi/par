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
	"crypto/md5"
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

func (h *Header) recalc(body []byte) {
	// MD5 Hash of packet. Used as a checksum for the packet.
	// Calculation starts at first byte of Recovery Set ID and
	// ends at last byte of body.
	// Does not include the magic sequence, length field or this field.
	// NB: The MD5 Hash, by its definition, includes the length as if it were appended to the packet.
	hsh := md5.New()
	n, _ := hsh.Write(h.RecoverySetID[:])
	m, _ := hsh.Write(h.Type[:])
	n += m
	m, _ = hsh.Write(body)
	n += m
	h.Length = uint64(n)
	binary.Write(hsh, binary.LittleEndian, h.Length)
	hsh.Sum(h.PacketMD5[:])
}

func (h *Header) writeTo(w io.Writer, body []byte) error {
	h.recalc(body)

	{
		w := &errWriter{w: w}
		w.Write(h.Sequence[:])
		binary.Write(w, binary.LittleEndian, h.Length)
		w.Write(h.PacketMD5[:])
		w.Write(h.RecoverySetID[:])
		w.Write(h.Type[:])
		return w.Err
	}
}

type errWriter struct {
	w   io.Writer
	Err error
}

func (ew *errWriter) Write(p []byte) (int, error) {
	if ew.Err != nil {
		return 0, ew.Err
	}
	n, err := ew.w.Write(p)
	ew.Err = err
	return n, err
}
