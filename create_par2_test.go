package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/tgulacsi/par/par2"
)

//go:generate go generate ./par2

func TestRecoveryPkt(t *testing.T) {
	want, err := par2.Stat("par2/testdata/input.txt.par2")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(want)

	out, err := ioutil.TempFile("", "par2-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(out.Name())
	defer out.Close()
	if err := VersionPAR2.CreateParFile(
		out.Name(), "par2/testdata/input.txt", 10, 3, int(want.Main.BlockSize),
	); err != nil {
		t.Fatal(err)
	}

	got := par2.ParInfo{
		ParFiles: []string{out.Name()},
	}
	if err := got.Parse(); err != nil {
		t.Fatal(err)
	}
	if g, w := len(got.Files), len(want.Files); g != w {
		t.Errorf("got %d files, wanted %d.", g, w)
	}
	if g, w := got.Files[0].FileDescPacket, want.Files[0].FileDescPacket; g.FileID != w.FileID || g.MD5 != w.MD5 {
		t.Errorf("got FileID %s, wanted %s.", g, w)
	}
	if g, w := got.Files[0].IFSCPacket, want.Files[0].IFSCPacket; !reflect.DeepEqual(g.Pairs, w.Pairs) {
		t.Errorf("got %v, wanted %v", g, w)
	}

	if g, w := got.RecoveryData, want.RecoveryData; !reflect.DeepEqual(g, w) {
		t.Errorf("got %v, wanted %v", g, w)
	}
}
