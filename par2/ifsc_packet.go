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
)

type IFSCPacket struct {
	*Header
	FileID MD5
	Pairs  []ChecksumPair
}

type ChecksumPair struct {
	MD5   MD5
	CRC32 CRC32
}

func (i *IFSCPacket) packetHeader() *Header {
	return i.Header
}

func (i *IFSCPacket) readBody(body []byte) {
	buff := bytes.NewBuffer(body)
	copy(i.FileID[:], buff.Next(16))

	pairCount := buff.Len() / 20
	i.Pairs = make([]ChecksumPair, 0, pairCount)
	for n := 0; n < pairCount; n++ {
		var pair ChecksumPair
		copy(pair.MD5[:], buff.Next(16))
		copy(pair.CRC32[:], buff.Next(4))
		i.Pairs = append(i.Pairs, pair)
	}
}

func (i *IFSCPacket) writeBody(dest []byte) []byte {
	buff := bytes.NewBuffer(dest)
	buff.Write(i.FileID[:])
	for _, pair := range i.Pairs {
		buff.Write(pair.MD5[:])
		buff.Write(pair.CRC32[:])
	}
	return buff.Bytes()
}