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
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	headerLength            uint64 = 0x40
	typeMainPacket          string = "PAR 2.0\000Main\000\000\000\000"
	typeFileDescPacket      string = "PAR 2.0\000FileDesc"
	typeIFSCPacket          string = "PAR 2.0\000IFSC\000\000\000\000"
	typeRecoverySlicePacket string = "PAR 2.0\000RecvSlic"
	typeCreatorPacket       string = "PAR 2.0\000Creator\000"
)

type Packet interface {
	readBody([]byte)
	writeBody([]byte) []byte
	packetHeader() *Header
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
type CRC32 [4]byte

func Stat(file string) (*ParInfo, error) {
	parFiles, err := allParFiles(file)
	if err != nil {
		return nil, err
	}

	stat := &ParInfo{
		ParFiles: parFiles,
	}
	packets, err := packets(stat.ParFiles)
	if err != nil {
		return nil, err
	}
	stat.Files = make([]*File, 0, len(packets))
	stat.RecoveryData = make([]*RecoverySlicePacket, 0, len(packets))

	stat.BaseDir = filepath.Dir(file)
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
				table[tmp.FileID] = &File{tmp, nil}
			}
		case *IFSCPacket:
			tmp := x
			stat.BlockCount += uint32(len(tmp.Pairs))
			if val, exists := table[tmp.FileID]; exists {
				val.IFSCPacket = tmp
				stat.Files = append(stat.Files, val)
			} else {
				table[tmp.FileID] = &File{nil, tmp}
			}
		}
	}

	return stat, nil
}

func Verify(info *ParInfo) {
	total_good := 0
	hash := md5.New()

	for _, file := range info.Files {
		fname := fmt.Sprintf("%s/%s", info.BaseDir, file.Filename)
		if _, err := os.Stat(fname); os.IsNotExist(err) {
			fmt.Printf("\t%s: missing\n", file.Filename)
			continue
		}

		good_blocks := 0
		f, _ := os.Open(fname)
		defer f.Close()

		for _, pair := range file.Pairs {
			buf := make([]byte, info.Main.BlockSize)
			f.Read(buf)
			hash.Write(buf)
			if bytes.Equal(hash.Sum(nil), pair.MD5[:]) {
				good_blocks++
			}
			hash.Reset()
		}
		total_good += good_blocks
		fmt.Printf("\t%s: %d/%d blocks available\n", file.Filename, good_blocks, len(file.Pairs))
	}
	missing := info.BlockCount - uint32(total_good)
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
	return filepath.Glob(dir + fname[:len(fname)-len(ext)] + ".*par2")
}

func packets(files []string) ([]Packet, error) {
	packets := make([]Packet, 0)
	for _, par := range files {
		f, err := os.Open(par)
		if err != nil {
			return nil, err
		}

		defer f.Close()
		stat, _ := f.Stat()
		par_size := stat.Size()

		for {
			h := new(Header)
			if err := h.fill(f); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			if !h.ValidSequence() {
				r, _ := f.Seek(-7, os.SEEK_CUR)
				if (par_size - r) < 8 {
					break
				}
				continue
			}

			buf := make([]byte, h.Length-headerLength)
			f.Read(buf)

			p := createPacket(h)
			verifyPacket(h, buf)
			p.readBody(buf)

			if !h.Damaged && !contains(packets, p) {
				packets = append(packets, p)
			}
		}
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

func createPacket(h *Header) Packet {
	switch string(h.Type[:]) {
	case typeMainPacket:
		return &MainPacket{Header: h}
	case typeFileDescPacket:
		return &FileDescPacket{Header: h}
	case typeIFSCPacket:
		return &IFSCPacket{Header: h}
	case typeRecoverySlicePacket:
		return &RecoverySlicePacket{Header: h}
	case typeCreatorPacket:
		return &CreatorPacket{Header: h}
	}

	return &UnknownPacket{h, nil}
}

func verifyPacket(h *Header, body []byte) {
	hash := md5.New()
	hash.Write(h.RecoverySetID[:])
	hash.Write(h.Type[:])
	hash.Write(body)

	h.Damaged = (len(body)%4) != 0 || !bytes.Equal(hash.Sum(nil), h.PacketMD5[:])
}
