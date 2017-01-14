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
	"testing"

	"github.com/kylelemons/godebug/diff"
)

func TestCR(t *testing.T) {
	inp, err := os.Open("main.go")
	if err != nil {
		t.Fatal(err)
	}
	defer inp.Close()

	parity, err := ioutil.TempFile("", "par-")
	defer remove(parity.Name())
	defer parity.Close()

	if err := VersionJSON.CreateParFile(parity.Name(), inp.Name(), 0, 0, 0); err != nil {
		t.Fatalf("create: %+v", err)
	}
	if _, err := inp.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind %q: %v", inp.Name(), err)
	}

	orig, err := ioutil.ReadAll(inp)
	if err != nil {
		t.Fatalf("read %q: %v", inp.Name(), err)
	}
	changed, err := ioutil.TempFile("", "par-")
	if err != nil {
		t.Fatal(err)
	}
	defer remove(changed.Name())
	n := len(orig) / 2
	changed.Write(orig[:n])
	changed.Write([]byte{orig[n] + 1})
	if _, err := changed.Write(orig[n+1:]); err != nil {
		t.Fatalf("write changed file %q: %v", changed.Name(), err)
	}
	if err := changed.Close(); err != nil {
		t.Fatal(err)
	}

	var restored bytes.Buffer
	if err := RestoreParFile(&restored, parity.Name(), inp.Name()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if d := diff.Diff(string(orig), restored.String()); d != "" {
		t.Error(d)
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
