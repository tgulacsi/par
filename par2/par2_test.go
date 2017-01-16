package par2

import "testing"

//go:generate par2create -r30 -s131072 -n1 -a testdata/input.txt.par2 testdata/input.txt
//go:generate par d testdata/input.txt.vol0+1.par2

func TestMD5(t *testing.T) {
	info, err := Stat("testdata/input.txt.par2")
	if err != nil {
		t.Fatal(err)
	}
	want := info.Creator.PacketMD5
	t.Log(want)
	b := bytesPool.Get()
	defer bytesPool.Put(b)
	cr := *info.Creator
	cr.PacketMD5 = MD5{}
	cr.Header.recalc(cr.writeBody(b))
	got := cr.PacketMD5
	if got != want {
		t.Errorf("got %s, wanted %s", got, want)
	}
}

func TestFileInfo(t *testing.T) {
	info, err := Stat("testdata/input.txt.par2")
	if err != nil {
		t.Fatal(err)
	}

	wFp := info.Files[0].FileDescPacket

	mb := NewMainBuilder()
	gFp, err := mb.AddFile("testdata/input.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := gFp.MiniMD5, wFp.MiniMD5; got != want {
		t.Errorf("got MiniMD5 %s, wanted %s", got, want)
	}
	if got, want := gFp.MD5, wFp.MD5; got != want {
		t.Errorf("got MD5 %s, wanted %s", got, want)
	}
	if got, want := gFp.FileID, wFp.FileID; got != want {
		t.Errorf("got FileID %s, wanted %s", got, want)
	}
}
