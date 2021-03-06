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
	"fmt"
)

type RecoverySlicePacket struct {
	Header
	Exponent     uint32
	RecoveryData []byte `json:"-"`
}

func (r RecoverySlicePacket) String() string {
	return fmt.Sprintf("%s-RECOV-%s, %d/%d", r.PacketMD5, r.RecoverySetID, r.Exponent, len(r.RecoveryData))
}

func (r *RecoverySlicePacket) packetHeader() Header {
	return r.Header
}

func (r *RecoverySlicePacket) readBody(body []byte) {
	binary.Read(bytes.NewReader(body), binary.LittleEndian, &r.Exponent)
	r.RecoveryData = body[4:]
}

func (r *RecoverySlicePacket) AvailableBlocks(blocksize uint64) uint64 {
	return uint64(len(r.RecoveryData)) / blocksize
}

func (r *RecoverySlicePacket) writeBody(dest []byte) []byte {
	buff := bytes.NewBuffer(dest[:0])
	binary.Write(buff, binary.LittleEndian, r.Exponent)
	buff.Write(r.RecoveryData)
	return buff.Bytes()
}
