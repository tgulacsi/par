package par2

import (
	"bytes"
	"testing"
)

func TestHeaderWriteTo(t *testing.T) {
	var buf bytes.Buffer

	var h Header
	h.SetType(TypeMainPacket)
	pkt := h.Create().(*MainPacket)
	_, err := pkt.AddFile("header_test.go")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := WritePacket(&buf, pkt); err != nil {
		t.Fatal(err)
	}
	t.Log(buf.String())

	var got Header
	if err := got.readFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Error(err)
	}
	if string(got.Type[:]) != string(TypeMainPacket) {
		t.Errorf("Got %s, wanted %#v.", got.Type, TypeMainPacket)
	}
}
