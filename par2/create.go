// Copyright (c) 2016 Tamás Gulácsi
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
	"crypto/md5"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type mainBuilder struct {
	Main            *MainPacket
	FileDescriptors []*FileDescPacket
	IFSCs           []*IFSCPacket
}

// NewMainBuilder returns a new writer which helps writing the needed packets.
//
// According to thee specification,
// http://parchive.sourceforge.net/docs/specifications/parity-volume-spec/article-spec.html#i__134603784_511
// 1. each packet has a header, which contains a checksum for the entire packet,
// including the recovery set id, the type, and the body of the packet.
//
// 2. "The MD5 hash of the body of the main packet is used as the Recovery Set ID",
// which is a hash of the slice size, the file count, and the file ids.
//
// 3. The File ID in this version is calculated as the MD5 Hash of the short MD5 hash
// of the file's first 16k, the length, and ASCII file name.
//
// So we need to know all the included files,
// calculate its IDs,
// put them in the Main packet,
// calculate the Recovery Set ID,
// put that into EVERY packet,
// calculate each header's hash, and go on.
func NewMainBuilder(blockSize int) *mainBuilder {
	m := CreatePacket(TypeMainPacket).(*MainPacket)
	m.BlockSize = uint64(blockSize)
	return &mainBuilder{Main: m}
}

func (pw *mainBuilder) AddFile(name string) (*FileDescPacket, *IFSCPacket, error) {
	fh, err := os.Open(name)
	if err != nil {
		return nil, nil, errors.Wrap(err, name)
	}
	fDesc, ifsc, err := pw.AddReader(name, fh)
	_ = fh.Close()
	return fDesc, ifsc, err
}

// AddReader adds the reader with the given filename to the recovery set.
//
// Creates the FileDescPacket and appends it to the Main packet's RecoverySetFileIDs.
// Also creates the IFSCPacket.
func (mb *mainBuilder) AddReader(name string, r io.Reader) (*FileDescPacket, *IFSCPacket, error) {
	h := mb.Main.Header
	h.SetType(TypeFileDescPacket)
	fDesc := h.Create().(*FileDescPacket)
	fDesc.FileName = filepath.Base(name)
	h.SetType(TypeIFSCPacket)
	ifsc := h.Create().(*IFSCPacket)

	hsh := md5.New()
	pw := NewChecksumPairWriter()
	cw := NewChunkWriter(pw, int(mb.Main.BlockSize))
	w := io.MultiWriter(hsh, cw)
	n, err := io.CopyN(w, r, 16<<10)
	fDesc.FileLength = uint64(n)
	hsh.Sum(fDesc.MiniMD5[:0])
	if err != nil {
		if err != io.EOF {
			return fDesc, ifsc, errors.Wrap(err, name)
		}
		fDesc.MD5 = fDesc.MiniMD5
	} else {
		if n, err = io.Copy(w, r); err != nil {
			return fDesc, ifsc, errors.Wrap(err, name)
		}
		fDesc.FileLength += uint64(n)
		hsh.Sum(fDesc.MD5[:0])
	}
	if err = cw.Close(); err != nil {
		return fDesc, ifsc, err
	}
	ifsc.Pairs = pw.Pairs
	fDesc.recalc()
	ifsc.FileID = fDesc.FileID
	mb.FileDescriptors = append(mb.FileDescriptors, fDesc)
	mb.Main.RecoverySetFileIDs = append(mb.Main.RecoverySetFileIDs, fDesc.FileID)

	return fDesc, ifsc, nil
}

// Finish the adding of new files, calculate the RecoverySetID and return the Main packet.
func (mb *mainBuilder) Finish() *MainPacket {
	b := bytesPool.Get()
	mb.Main.writeBody(b)
	bytesPool.Put(b)

	for _, fDesc := range mb.FileDescriptors {
		fDesc.RecoverySetID = mb.Main.RecoverySetID
	}
	for _, ifsc := range mb.IFSCs {
		ifsc.RecoverySetID = mb.Main.RecoverySetID
	}

	return mb.Main
}

type chunkWriter struct {
	io.Writer
	Size int
	buf  []byte
}

func NewChunkWriter(w io.Writer, size int) *chunkWriter {
	return &chunkWriter{Writer: w, Size: size, buf: make([]byte, size)}
}
func (w *chunkWriter) Write(p []byte) (int, error) {
	n := len(p)
	for {
		i := w.Size - len(w.buf)
		if i > len(p) {
			w.buf = append(w.buf, p...)
			return n, nil
		}
		w.buf = append(w.buf, p[:i]...)
		p = p[i:]
		_, err := w.Writer.Write(w.buf)
		w.buf = w.buf[:0]
		if err != nil {
			return n, err
		}
	}
}
func (w *chunkWriter) Close() error {
	if len(w.buf) == 0 {
		return nil
	}
	n, err := w.Write(w.buf)
	w.buf = w.buf[len(w.buf)-n:]
	return err
}

type checksumPairWriter struct {
	md5   hash.Hash
	crc   hash.Hash32
	Pairs []ChecksumPair
}

func NewChecksumPairWriter() *checksumPairWriter {
	return &checksumPairWriter{md5: md5.New(), crc: crc32.NewIEEE()}
}
func (w *checksumPairWriter) Write(p []byte) (int, error) {
	n := len(p)
	var pair ChecksumPair

	w.md5.Reset()
	_, err := w.md5.Write(p)
	if err != nil {
		return n, err
	}
	w.md5.Sum(pair.MD5[:0])

	w.crc.Reset()
	_, err = w.crc.Write(p)
	if err != nil {
		return n, err
	}
	w.crc.Sum(pair.CRC32[:0])

	w.Pairs = append(w.Pairs, pair)
	return n, nil
}
