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
	"io"
)

type FileDescPacket struct {
	Header
	FileID     MD5
	MD5        MD5
	MiniMD5    MD5
	FileLength uint64
	FileName   string
}

func (f FileDescPacket) String() string {
	return fmt.Sprintf("%q %db [%s]", f.FileName, f.FileLength, f.FileID)
}

func (f *FileDescPacket) packetHeader() Header {
	return f.Header
}

func (f *FileDescPacket) readBody(body []byte) {
	copy(f.FileID[:], body)
	body = body[16:]
	copy(f.MD5[:], body)
	body = body[16:]
	copy(f.MiniMD5[:], body)
	body = body[16:]
	f.FileLength = binary.LittleEndian.Uint64(body)
	body = body[8:]
	f.FileName = string(bytes.TrimRight(body, "\000"))
}

func (f *FileDescPacket) recalc() {
	// The File ID in this version is calculated as the MD5 Hash of the last 3 fields of the body of this packet:
	// MD5-16k, length, and ASCII file name.
	// Note: The length and MD5-16k are included because the Recovery Set ID is a hash of the File IDs
	// and the Recovery Set ID should be a function of file contents as well as names.

	hsh := md5.New()
	hsh.Write(f.MiniMD5[:])
	binary.Write(hsh, binary.LittleEndian, f.FileLength)
	// According to the spec, the file name is extended to 4-dividable length with zero bytes.
	// But according to my tests, par does not count those zeroes into the hash!
	//writeString(hsh, f.FileName)
	io.WriteString(hsh, f.FileName)

	hsh.Sum(f.FileID[:0])
}

func writeString(w io.Writer, s string) (int, error) {
	n, err := io.WriteString(w, s)
	if err != nil {
		return n, err
	}
	if k := n % 4; k != 0 {
		k, err = w.Write([]byte{0, 0, 0}[:4-k])
		n += k
	}
	return n, err
}

func (f *FileDescPacket) writeBody(dest []byte) []byte {
	if f.FileID.IsZero() {
		f.recalc()
	}

	buff := bytes.NewBuffer(dest[:0])
	buff.Write(f.FileID[:])
	buff.Write(f.MD5[:])
	buff.Write(f.MiniMD5[:])
	binary.Write(buff, binary.LittleEndian, f.FileLength)
	writeString(buff, f.FileName)
	return buff.Bytes()
}
