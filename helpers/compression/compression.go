// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package compression

import (
	"bytes"
	"compress/gzip"

	"github.com/featureform/fferr"
)

func GunZip(message []byte) (string, error) {
	// this function takes a gzip compressed byte array and
	// uncompresses it into string

	buffer := bytes.NewBuffer(message)

	gr, err := gzip.NewReader(buffer)
	if err != nil {
		return "", fferr.NewInternalError(err)
	}
	defer gr.Close()

	var uncompressed bytes.Buffer
	if _, err := uncompressed.ReadFrom(gr); err != nil {
		return "", fferr.NewInternalError(err)
	}

	return uncompressed.String(), nil
}
