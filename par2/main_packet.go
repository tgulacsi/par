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
	"fmt"
	"sort"
)

type MainPacket struct {
	Header
	BlockSize             uint64
	RecoverySetCount      uint32
	RecoverySetFileIDs    []MD5
	NonRecoverySetFileIDs []MD5
}

func (m MainPacket) String() string {
	return fmt.Sprintf(`%s
BlockSize: %d
RecoverSetCount: %d
RecoverSetFileIDs: %d
NonRecoverySetFileIDs: %d`,
		m.Header, m.BlockSize, m.RecoverySetCount,
		len(m.RecoverySetFileIDs), len(m.NonRecoverySetFileIDs))
}

func (m *MainPacket) packetHeader() Header {
	return m.Header
}

func (m *MainPacket) readBody(body []byte) {
	m.BlockSize = binary.LittleEndian.Uint64(body)
	body = body[8:]
	m.RecoverySetCount = binary.LittleEndian.Uint32(body)
	body = body[4:]

	nonRecCount := (len(body) >> 4) - int(m.RecoverySetCount)
	m.RecoverySetFileIDs = make([]MD5, m.RecoverySetCount)
	m.NonRecoverySetFileIDs = make([]MD5, nonRecCount)
	for _, arr := range [][]MD5{m.RecoverySetFileIDs, m.NonRecoverySetFileIDs} {
		for i := range arr {
			copy(arr[i][:], body)
			body = body[16:]
		}
	}
}

// "The MD5 hash of the body of the main packet is used as the Recovery Set ID",
// which is a hash of the slice size, the file count, and the file ids.
func (m *MainPacket) writeBody(dest []byte) []byte {
	m.RecoverySetCount = uint32(len(m.RecoverySetFileIDs))

	buff := bytes.NewBuffer(dest[:0])
	binary.Write(buff, binary.LittleEndian, &m.BlockSize)
	binary.Write(buff, binary.LittleEndian, &m.RecoverySetCount)

	for _, arr := range [][]MD5{m.RecoverySetFileIDs, m.NonRecoverySetFileIDs} {
		for _, fid := range arr {
			buff.Write(fid[:])
		}
	}
	hsh := md5.New()
	hsh.Write(buff.Bytes())
	hsh.Sum(m.Header.RecoverySetID[:0])
	return buff.Bytes()
}

// sortMD5s sorts by numerical value (treating them as 16-byte unsigned integers).
func sortMD5s(p []MD5) {
	if len(p) < 2 {
		return
	}
	sort.Slice(p, func(i, j int) bool {
		var a, b MD5
		// reverse from Little-Endian to Big-Endian
		for i, c := range p[i] {
			a[15-i] = c
		}
		for i, c := range p[j] {
			b[15-i] = c
		}

		return bytes.Compare(a[:], b[:]) == -1
	})
}
