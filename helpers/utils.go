package helpers

import "math/rand"

func generate_unique_request_id() int32 {
	return int32(rand.Uint32())
}
