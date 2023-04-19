package decompress

import (
	"bytes"
	"compress/gzip"
	"fmt"
)

func GZipMessageToString(message []byte) (string, error) {
	// this function takes a gzip compressed byte array and
	// decompresses it into string

	buffer := bytes.NewBuffer(message)

	gr, err := gzip.NewReader(buffer)
	if err != nil {
		return "", fmt.Errorf("could not create a new gzip Reader: %v", err)
	}

	var decompressed bytes.Buffer
	if _, err := decompressed.ReadFrom(gr); err != nil {
		return "", fmt.Errorf("could not read from gzip Reader: %v", err)
	}

	if err := gr.Close(); err != nil {
		return "", fmt.Errorf("could not close gzip Reader: %v", err)
	}
	return decompressed.String(), nil
}
