// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
)

func GunZip(message []byte) (string, error) {
	// this function takes a gzip compressed byte array and
	// uncompresses it into string

	buffer := bytes.NewBuffer(message)

	gr, err := gzip.NewReader(buffer)
	if err != nil {
		return "", fmt.Errorf("could not create a new gzip Reader: %v", err)
	}
	defer gr.Close()

	var uncompressed bytes.Buffer
	if _, err := uncompressed.ReadFrom(gr); err != nil {
		return "", fmt.Errorf("could not read from gzip Reader: %v", err)
	}

	return uncompressed.String(), nil
}
