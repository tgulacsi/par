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

// Package par2 is for reading/writing PAR2 archives.
// Definition:
// http://parchive.sourceforge.net/docs/specifications/parity-volume-spec/article-spec.html
package par2

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

const (
	headerLength uint64 = 0x40

	TypeMainPacket          = PacketType("PAR 2.0\000Main\000\000\000\000")
	TypeFileDescPacket      = PacketType("PAR 2.0\000FileDesc")
	TypeIFSCPacket          = PacketType("PAR 2.0\000IFSC\000\000\000\000")
	TypeRecoverySlicePacket = PacketType("PAR 2.0\000RecvSlic")
	TypeCreatorPacket       = PacketType("PAR 2.0\000Creator\000")
)

type PacketType string

type Packet interface {
	readBody([]byte)
	writeBody([]byte) []byte
	packetHeader() Header
}

type ParInfo struct {
	Main         *MainPacket
	Creator      *CreatorPacket
	Files        []*File
	RecoveryData []*RecoverySlicePacket
	ParFiles     []string
	BlockCount   uint32
	TotalSize    uint64
	BaseDir      string
}

type MD5 [16]byte

func (m MD5) String() string { return base64.URLEncoding.EncodeToString(m[:]) }
func (m MD5) MarshalText() ([]byte, error) {
	var a [24]byte
	base64.URLEncoding.Encode(a[:], m[:])
	return a[:], nil
}
func (m MD5) MarshalBinary() ([]byte, error) { return m[:], nil }
func (m MD5) IsZero() bool                   { return m == MD5{} }

type CRC32 [4]byte

func (m CRC32) String() string { return base64.URLEncoding.EncodeToString(m[:]) }
func (m CRC32) MarshalText() ([]byte, error) {
	var a [8]byte
	base64.URLEncoding.Encode(a[:], m[:])
	return a[:], nil
}
func (m CRC32) MarshalBinary() ([]byte, error) { return m[:], nil }

func Stat(file string) (*ParInfo, error) {
	parFiles, err := allParFiles(file)
	if err != nil {
		return nil, errors.WithMessage(err, "list par files")
	}
	if len(parFiles) == 0 {
		return nil, errors.New("No par file found")
	}

	stat := &ParInfo{
		ParFiles: parFiles,
	}

	return stat, stat.Parse()
}

func (stat *ParInfo) Parse() error {
	packets, err := readPackets(nil, stat.ParFiles)
	if err != nil {
		return errors.WithMessage(err, "read packets")
	}
	stat.Files = make([]*File, 0, len(packets))
	stat.RecoveryData = make([]*RecoverySlicePacket, 0, len(packets))

	stat.BaseDir = filepath.Dir(stat.ParFiles[0])
	table := make(map[MD5]*File)
	for _, p := range packets {
		switch x := p.(type) {
		case *MainPacket:
			stat.Main = x
		case *CreatorPacket:
			stat.Creator = x
		case *RecoverySlicePacket:
			stat.RecoveryData = append(stat.RecoveryData, x)
		case *FileDescPacket:
			tmp := x
			stat.TotalSize += tmp.FileLength
			if val, exists := table[tmp.FileID]; exists {
				val.FileDescPacket = tmp
				stat.Files = append(stat.Files, val)
			} else {
				table[tmp.FileID] = &File{FileDescPacket: tmp}
			}
		case *IFSCPacket:
			tmp := x
			stat.BlockCount += uint32(len(tmp.Pairs))
			if val, exists := table[tmp.FileID]; exists {
				val.IFSCPacket = tmp
				stat.Files = append(stat.Files, val)
			} else {
				table[tmp.FileID] = &File{IFSCPacket: tmp}
			}
		}
	}

	return nil
}

func Verify(info *ParInfo) {
	totalGood := 0
	hash := md5.New()
	var hshBuf MD5

FilesLoop:
	for _, file := range info.Files {
		fname := fmt.Sprintf("%s/%s", info.BaseDir, file.FileName)
		if _, err := os.Stat(fname); os.IsNotExist(err) {
			fmt.Printf("\t%s: missing\n", file.FileName)
			continue
		}

		goodBlocks := 0
		f, err := os.Open(fname)
		if err != nil {
			fmt.Printf("\t%s: open: %v\n", fname, err)
			continue
		}

		for _, pair := range file.Pairs {
			if _, err := io.CopyN(hash, f, int64(info.Main.BlockSize)); err != nil {
				fmt.Printf("\t%s: read: %v\n", fname, err)
				f.Close()
				continue FilesLoop
			}
			hash.Sum(hshBuf[:0])
			if hshBuf == pair.MD5 {
				goodBlocks++
			}
			hash.Reset()
		}
		totalGood += goodBlocks
		f.Close()

		fmt.Printf("\t%s: %d/%d blocks available\n", file.FileName, goodBlocks, len(file.Pairs))
	}
	missing := info.BlockCount - uint32(totalGood)
	fmt.Printf("\t-------\n\t%d missing blocks, %d recovery blocks: ", missing, len(info.RecoveryData))

	if missing == 0 {
		fmt.Println("Repair not needed.")
	} else if missing > uint32(len(info.RecoveryData)) {
		fmt.Println("Repair not possible.")
	} else {
		fmt.Println("Repair is required.")
	}
}

func allParFiles(file string) ([]string, error) {
	dir, fname := filepath.Split(file)
	ext := filepath.Ext(fname)
	glob := dir + fname[:len(fname)-len(ext)] + ".*par2"
	files, err := filepath.Glob(glob)
	return files, errors.Wrap(err, glob)
}

func readPackets(packets []Packet, files []string) ([]Packet, error) {
	if len(files) == 0 {
		log.Printf("No files provided.")
		return nil, nil
	}
	packets = packets[:0]
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	for _, par := range files {
		f, err := os.Open(par)
		if err != nil {
			return packets, errors.Wrap(err, par)
		}

		defer f.Close()
		stat, err := f.Stat()
		if err != nil {
			return packets, errors.Wrap(err, "stat "+f.Name())
		}
		parSize := stat.Size()

		for {
			var h Header
			if err := h.readFrom(f); err == io.EOF {
				break
			} else if err != nil {
				return packets, errors.Wrapf(err, "readFrom %q", f.Name())
			}
			if !h.ValidSequence() {
				r, err := f.Seek(-7, io.SeekCurrent)
				if err != nil {
					return packets, errors.Wrap(err, "Seek -7")
				}
				if (parSize - r) < 8 {
					break
				}
				continue
			}

			n := int(h.Length - headerLength)
			if cap(buf) < n {
				buf = make([]byte, n)
				defer bytesPool.Put(buf)
			} else {
				buf = buf[:n]
			}
			if _, err := io.ReadFull(f, buf); err != nil {
				return packets, errors.Wrapf(err, "read %d bytes from %q", n, f.Name())
			}

			p := h.Create()
			h.verifyPacket(buf)
			p.readBody(buf)

			if h.Damaged || contains(packets, p) {
				continue
			}
			packets = append(packets, p)
		}
		f.Close()
	}

	return packets, nil
}

func contains(packets []Packet, packet Packet) bool {
	header := packet.packetHeader()
	for _, p := range packets {
		h := p.packetHeader()
		if h.PacketMD5 == header.PacketMD5 {
			return true
		}
	}
	return false
}

// Create a packet containing this Header, with the proper type.
func (h Header) Create() Packet {
	switch PacketType(h.Type[:]) {
	case TypeMainPacket:
		m := MainPacket{Header: h}
		if _, err := rand.Read(m.RecoverySetID[:]); err != nil {
			panic(err)
		}
		return &m
	case TypeFileDescPacket:
		return &FileDescPacket{Header: h}
	case TypeIFSCPacket:
		return &IFSCPacket{Header: h}
	case TypeRecoverySlicePacket:
		return &RecoverySlicePacket{Header: h}
	case TypeCreatorPacket:
		return &CreatorPacket{Header: h}
	}

	return &UnknownPacket{Header: h}
}

func CreatePacket(typ PacketType) Packet {
	var h Header
	copy(h.Type[:], []byte(typ[:16]))
	return h.Create()
}

func WritePacket(w io.Writer, p Packet) (int64, error) {
	b := bytesPool.Get()
	defer bytesPool.Put(b)
	return p.(interface {
		writeTo(io.Writer, []byte) (int64, error)
	}).writeTo(w, p.writeBody(b))
}

func (h Header) verifyPacket(body []byte) {
	hash := md5.New()
	hash.Write(h.RecoverySetID[:])
	hash.Write(h.Type[:])
	hash.Write(body)

	b := bytesPool.Get()
	defer bytesPool.Put(b)
	h.Damaged = (len(body)%4) != 0 || !bytes.Equal(hash.Sum(b), h.PacketMD5[:])
}

var bytesPool = byteSlices{Pool: sync.Pool{New: func() interface{} { return make([]byte, 0, 1024) }}}

type byteSlices struct {
	sync.Pool
}

func (bs byteSlices) Get() []byte {
	return bs.Pool.Get().([]byte)[:0]
}
func (bs byteSlices) Put(p []byte) {
	if cap(p) == 0 {
		return
	}
	bs.Pool.Put(p[:0])
}
