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
	"sort"
)

type MainPacket struct {
	*Header
	BlockSize             uint64
	RecoverySetCount      uint32
	RecoverySetFileIDs    []MD5
	NonRecoverySetFileIDs []MD5
}

func (m *MainPacket) packetHeader() *Header {
	return m.Header
}

func (m *MainPacket) readBody(body []byte) {
	buff := bytes.NewBuffer(body)
	binary.Read(buff, binary.LittleEndian, &m.BlockSize)
	binary.Read(buff, binary.LittleEndian, &m.RecoverySetCount)

	m.RecoverySetFileIDs = make([]MD5, m.RecoverySetCount)
	nonRecCount := buff.Len() / 16
	m.NonRecoverySetFileIDs = make([]MD5, nonRecCount)
	for _, arr := range [][]MD5{m.RecoverySetFileIDs, m.NonRecoverySetFileIDs} {
		for i := range arr {
			copy(arr[i][:], buff.Next(16))
		}
	}
}

func (m *MainPacket) writeBody(dest []byte) []byte {
	buff := bytes.NewBuffer(dest)
	binary.Write(buff, binary.LittleEndian, &m.BlockSize)
	binary.Write(buff, binary.LittleEndian, &m.RecoverySetCount)

	for _, arr := range [][]MD5{m.RecoverySetFileIDs, m.NonRecoverySetFileIDs} {
		sortMD5s(arr)
		for _, fid := range arr {
			buff.Write(fid[:])
		}
	}
	return buff.Bytes()
}

func (m *MainPacket) WriteTo(w io.Writer) (int64, error) {
	return m.Header.writeTo(w, m.writeBody(nil))
}

// sortMD5s sorts by numerical value (treating them as 16-byte unsigned integers).
func sortMD5s(p []MD5) {
	sort.Slice(p, func(i, j int) bool { return bytes.Compare(p[i][:], p[j][:]) == -1 })
}
