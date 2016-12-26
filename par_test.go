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
	"io/ioutil"
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

	fh, err := ioutil.TempFile("", "par-")
	defer os.Remove(fh.Name())
	defer fh.Close()

	if err := CreateParFile(fh.Name(), inp.Name(), 0, 0, 0); err != nil {
		t.Fatalf("create: %+v", err)
	}

	var restored bytes.Buffer
	if err := RestoreParFile(&restored, fh.Name(), inp.Name(), 0, 0, 0); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	orig, err := ioutil.ReadAll(inp)
	if err != nil {
		t.Fatalf("read %q: %v", inp.Name(), err)
	}

	if d := diff.Diff(string(orig), restored.String()); d != "" {
		t.Error(d)
	}
}
