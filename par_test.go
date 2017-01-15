// Copyright 2016 Tamás Gulácsi
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/kylelemons/godebug/diff"
	"github.com/pkg/errors"
)

var errFatal = errors.New("fatal")

func TestCR(t *testing.T) {
	inp, err := os.Open("main.go")
	if err != nil {
		t.Fatal(err)
	}
	defer inp.Close()

	parity, err := ioutil.TempFile("", "par-")
	defer remove(parity.Name())
	defer parity.Close()

	for _, ver := range []version{VersionJSON, VersionTAR, VersionPAR2} {
		if _, err := inp.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		testCR(t, ver, parity.Name(), inp)
	}
}

func testCR(t *testing.T, ver version, parityName string, inp *os.File) {
	if err := ver.CreateParFile(parityName, inp.Name(), 0, 0, 0); err != nil {
		t.Fatalf("%s. %+v", ver, err)
	}
	if _, err := inp.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("%s. rewind %q: %v", ver, inp.Name(), err)
	}

	orig, err := ioutil.ReadAll(inp)
	if err != nil {
		t.Fatalf("%s. read %q: %v", ver, inp.Name(), err)
	}
	changed, err := ioutil.TempFile("", "par-")
	if err != nil {
		t.Fatalf("%s. %v", ver, err)
	}
	defer remove(changed.Name())
	n := len(orig) / 2
	changed.Write(orig[:n])
	changed.Write([]byte{orig[n] + 1})
	if _, err := changed.Write(orig[n+1:]); err != nil {
		t.Fatalf("%s. write changed file %q: %v", ver, changed.Name(), err)
	}
	if err := changed.Close(); err != nil {
		t.Fatalf("%s. %v", err)
	}

	var restored bytes.Buffer
	if err := RestoreParFile(&restored, parityName, inp.Name()); err != nil {
		t.Fatalf("%s. Restore: %v", ver, err)
	}

	if d := strings.TrimSuffix(
		diff.Diff(string(orig), restored.String()),
		"-\n+",
	); d != "" {
		t.Logf("%s. LENGTH: got %d, wanted %d; END: %q\n",
			ver, len(restored.String()), len(orig), d[len(d)-10:])
		t.Errorf("%s. %s", ver, d)
	}
}

var KeepFiles = os.Getenv("KEEP_FILES") == "1"

func remove(fn string) error {
	if KeepFiles {
		log.Println("KEEP", fn)
		return nil
	}
	return os.Remove(fn)
}
