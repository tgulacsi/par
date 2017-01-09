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
	"fmt"
)

type IFSCPacket struct {
	Header
	FileID MD5
	Pairs  []ChecksumPair
}

func (i IFSCPacket) String() string {
	return fmt.Sprintf("[%s] #pairs=%d", i.FileID, len(i.Pairs))
}

type ChecksumPair struct {
	MD5   MD5
	CRC32 CRC32
}

func (i *IFSCPacket) packetHeader() Header {
	return i.Header
}

func (i *IFSCPacket) readBody(body []byte) {
	copy(i.FileID[:], body)
	body = body[16:]

	pairCount := len(body) / 20
	i.Pairs = make([]ChecksumPair, 0, pairCount)
	for n := 0; n < pairCount; n++ {
		var pair ChecksumPair
		copy(pair.MD5[:], body)
		body = body[16:]
		copy(pair.CRC32[:], body)
		body = body[4:]
		i.Pairs = append(i.Pairs, pair)
	}
}

func (i *IFSCPacket) writeBody(dest []byte) []byte {
	buff := bytes.NewBuffer(dest[:0])
	buff.Write(i.FileID[:])
	for _, pair := range i.Pairs {
		buff.Write(pair.MD5[:])
		buff.Write(pair.CRC32[:])
	}
	return buff.Bytes()
}
