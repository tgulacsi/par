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
	"fmt"
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
		if err := testCR(ver, parity.Name(), inp); err != nil {
			if errors.Cause(err) == errFatal {
				t.Fatal("%s: %+v", ver, err)
			}
			t.Errorf("%s: %v", ver, err)
		}
	}
}

func testCR(ver version, parityName string, inp *os.File) error {
	if err := ver.CreateParFile(parityName, inp.Name(), 0, 0, 0); err != nil {
		return errors.Wrapf(errFatal, "create: %+v", err)
	}
	if _, err := inp.Seek(0, io.SeekStart); err != nil {
		return errors.Wrapf(errFatal, "rewind %q: %v", inp.Name(), err)
	}

	orig, err := ioutil.ReadAll(inp)
	if err != nil {
		return errors.Wrapf(errFatal, "read %q: %v", inp.Name(), err)
	}
	changed, err := ioutil.TempFile("", "par-")
	if err != nil {
		return errors.Wrapf(errFatal, "%+v", err)
	}
	defer remove(changed.Name())
	n := len(orig) / 2
	changed.Write(orig[:n])
	changed.Write([]byte{orig[n] + 1})
	if _, err := changed.Write(orig[n+1:]); err != nil {
		return errors.Wrapf(errFatal, "write changed file %q: %v", changed.Name(), err)
	}
	if err := changed.Close(); err != nil {
		return errors.Wrapf(errFatal, "%+v", err)
	}

	var restored bytes.Buffer
	if err := RestoreParFile(&restored, parityName, inp.Name()); err != nil {
		return errors.Wrapf(errFatal, "Restore: %v", err)
	}

	if d := strings.TrimSuffix(
		diff.Diff(string(orig), restored.String()),
		"-\n+",
	); d != "" {
		fmt.Fprintf(os.Stderr, "LENGTH: got %d, wanted %d; END: %q\n",
			len(restored.String()), len(orig), d[len(d)-10:])
		return errors.New(d)
	}
	return nil
}

var KeepFiles = os.Getenv("KEEP_FILES") == "1"

func remove(fn string) error {
	if KeepFiles {
		log.Println("KEEP", fn)
		return nil
	}
	return os.Remove(fn)
}
