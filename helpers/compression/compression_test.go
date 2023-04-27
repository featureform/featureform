//go:build compress
// +build compress

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package compression

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func TestUncompressMessage(t *testing.T) {
	type TestCase struct {
		InputString string
	}

	tests := map[string]TestCase{
		"hiWorld": {
			InputString: "hi world",
		},
		"featureStore": {
			InputString: "featureform is a virtual feature store",
		},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		inputByte := []byte(test.InputString)

		// create a buffer to hold the compressed data
		var compressed bytes.Buffer

		// create a gzip writer that writes to the buffer
		gz := gzip.NewWriter(&compressed)

		// write the inputByte to the gzip writer
		if _, err := gz.Write(inputByte); err != nil {
			t.Fatal(err)
		}

		// close the gzip writer to flush any remaining data
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}

		// print the compressed data as a byte slice
		compressedBytes := compressed.Bytes()

		outputString, err := GunZip(compressedBytes)
		if err != nil {
			t.Fatalf("could not uncompress message: %v", err)
		}

		if outputString != test.InputString {
			t.Fatalf("strings do not match: expected '%s' but got '%s'", test.InputString, outputString)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runTestCase(t, test)
		})
	}
}
