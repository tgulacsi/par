package par2

import "testing"

//go:generate rm testdata/input.txt.vol*.par2
//go:generate par2create -r30 -s2048 -n1 -a testdata/input.txt.par2 testdata/input.txt
// go:generate par d testdata/input.txt.vol0+1.par2

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

	wIp := info.Files[0].IFSCPacket
	wFp := info.Files[0].FileDescPacket

	mb := NewMainBuilder(int(info.Main.BlockSize))
	gFp, gIp, err := mb.AddFile("testdata/input.txt")
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

	b := bytesPool.Get()
	defer bytesPool.Put(b)
	gFp.Header.RecoverySetID = wFp.RecoverySetID
	gFp.Header.recalc(gFp.writeBody(b))
	if got, want := gFp.PacketMD5, wFp.PacketMD5; got != want {
		t.Errorf("got PacketMD5 %s, wanted %s", got, want)
	}
	if gIp.FileID != wIp.FileID {
		t.Errorf("FileID mismatch: got %s wanted %s.", gIp.FileID, wIp.FileID)
	}
	g, w := len(gIp.Pairs), len(wIp.Pairs)
	if g != w {
		t.Errorf("got %d ifsc pairs, wanted %d", g, w)
		if g > w {
			g = w
		}
	}
	for i := 0; i < g; i++ {
		got, want := gIp.Pairs[i], wIp.Pairs[i]
		if got != want {
			t.Errorf("%d. got %v, wanted %v", i, got, want)
		}
	}
}
