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

	if _, err := WritePacket(&buf, pkt); err != nil {
		t.Fatal(err)
	}
	t.Log(buf.String())

	h = Header{}
	if err := h.readFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Error(err)
	}
}
