package main

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
	"github.com/tgulacsi/par/par2"
)

const Creator = "github.com/tgulacsi/par"

var _ = io.WriteCloser((*rsPAR2Writer)(nil))

type rsPAR2Writer struct {
	rsEnc
	w      io.Writer
	meta   FileMetadata
	Header par2.Header
	FileID par2.MD5
	ifsc   *par2.IFSCPacket
}

func NewPAR2Writer(w io.Writer, meta FileMetadata) (*rsPAR2Writer, error) {
	prw := rsPAR2Writer{w: w}
	prw.rsEnc = meta.newRSEnc(prw.writeShards)
	prw.meta = meta
	fh, err := os.Open(meta.FileName)
	if err != nil {
		return nil, errors.Wrap(err, meta.FileName)
	}
	mainPkt := par2.CreatePacket(par2.TypeMainPacket).(*par2.MainPacket)
	fDescPkt, err := mainPkt.Add(fh, meta.FileName)
	fh.Close()
	if err != nil {
		return nil, err
	}
	prw.Header = mainPkt.Header
	prw.FileID = fDescPkt.FileID

	crPkt := par2.CreatePacket(par2.TypeCreatorPacket).(*par2.CreatorPacket)
	crPkt.Creator = Creator
	for _, p := range []par2.Packet{crPkt, mainPkt, fDescPkt} {
		if _, err := par2.WritePacket(w, p); err != nil {
			return nil, err
		}
	}
	return &prw, nil
}

func (rw *rsPAR2Writer) Close() error {
	err := rw.WriteShards()
	if err != nil {
		return err
	}
	_, err = par2.WritePacket(rw.w, rw.ifsc)
	return err
}

func (rw *rsPAR2Writer) writeShards(slices [][]byte, length int) error {
	h := rw.Header
	if rw.ifsc == nil {
		h.SetType(par2.TypeIFSCPacket)
		rw.ifsc = h.Create().(*par2.IFSCPacket)
		rw.ifsc.FileID = rw.FileID
	}
	h.SetType(par2.TypeRecoverySlicePacket)
	recov := *(h.Create().(*par2.RecoverySlicePacket))

	hshCRC, hshMD5 := crc32.NewIEEE(), md5.New()
	log.Printf("writeShards(%d, %d) ds=%d", len(slices), length, rw.meta.DataShards)
	for i, b := range slices {
		isDataShard := i < int(rw.meta.DataShards)
		if isDataShard {
			if len(b) > length {
				b = b[:length]
			}
			length -= len(b)

			var cp par2.ChecksumPair
			hshCRC.Reset()
			hshCRC.Write(b)
			hshCRC.Sum(cp.CRC32[:0])
			hshMD5.Reset()
			hshMD5.Write(b)
			hshMD5.Sum(cp.MD5[:0])
			rw.ifsc.Pairs = append(rw.ifsc.Pairs, cp)

			continue
		}

		// parity shard
		recov.RecoveryData = b
		if _, err := par2.WritePacket(rw.w, &recov); err != nil {
			return err
		}
	}
	return nil
}
