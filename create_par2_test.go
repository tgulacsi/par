package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/tgulacsi/par/par2"
)

//go:generate go generate ./par2

func TestRecoveryPkt(t *testing.T) {
	info, err := par2.Stat("par2/testdata/input.txt.par2")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(info)

	out, err := ioutil.TempFile("", "par2-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(out.Name())
	defer out.Close()
	if err := VersionPAR2.CreateParFile(
		out.Name(), "par2/testdata/input.txt", 10, 3, int(info.Main.BlockSize),
	); err != nil {
		t.Fatal(err)
	}
}
