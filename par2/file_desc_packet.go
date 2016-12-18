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
)

type FileDescPacket struct {
	*Header
	FileID     MD5
	MD5        MD5
	MiniMD5    MD5
	FileLength uint64
	Filename   string
}

func (f *FileDescPacket) packetHeader() *Header {
	return f.Header
}

func (f *FileDescPacket) readBody(body []byte) {
	buff := bytes.NewBuffer(body)
	copy(f.FileID[:], buff.Next(16))
	copy(f.MD5[:], buff.Next(16))
	copy(f.MiniMD5[:], buff.Next(16))
	binary.Read(buff, binary.LittleEndian, &f.FileLength)
	f.Filename = string(bytes.TrimRight(buff.Next(buff.Len()), "\000"))
}

func (f *FileDescPacket) writeBody(dest []byte) []byte {
	buff := bytes.NewBuffer(dest)
	buff.Write(f.FileID[:])
	buff.Write(f.MD5[:])
	buff.Write(f.MiniMD5[:])
	binary.Write(buff, binary.LittleEndian, f.FileLength)
	buff.WriteString(f.Filename)
	for n := len(f.Filename) % 4; n != 0; n-- {
		if n == 0 {
			break
		}
		buff.WriteByte(0)
	}
	return buff.Bytes()
}
