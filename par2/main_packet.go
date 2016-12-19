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
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
)

type MainPacket struct {
	Header
	BlockSize             uint64
	RecoverySetCount      uint32
	RecoverySetFileIDs    []MD5
	NonRecoverySetFileIDs []MD5
}

func (m *MainPacket) packetHeader() Header {
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
	m.RecoverySetCount = uint32(len(m.RecoverySetFileIDs))

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

func (m *MainPacket) AddFile(fileName string) (*FileDescPacket, error) {
	fh, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrap(err, fileName)
	}
	defer fh.Close()
	return m.Add(fh, fh.Name())
}

func (m *MainPacket) Add(r io.Reader, name string) (*FileDescPacket, error) {
	h := m.Header
	h.SetType(TypeFileDescPacket)
	fDesc := h.Create().(*FileDescPacket)
	fDesc.FileName = filepath.Base(name)

	h.SetType(TypeIFSCPacket)
	hsh := md5.New()
	n, err := io.CopyN(hsh, r, 16<<10)
	fDesc.FileLength = uint64(n)
	hsh.Sum(fDesc.MiniMD5[:])
	if err != nil {
		if err != io.EOF {
			return fDesc, errors.Wrap(err, name)
		}
		fDesc.MD5 = fDesc.MiniMD5
	} else {
		if n, err = io.Copy(hsh, r); err != nil {
			return fDesc, errors.Wrap(err, name)
		}
		fDesc.FileLength += uint64(n)
		hsh.Sum(fDesc.MD5[:])
	}
	fDesc.recalc()
	m.RecoverySetFileIDs = append(m.RecoverySetFileIDs, fDesc.FileID)
	return fDesc, nil
}
