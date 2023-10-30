package lib

import (
	"github.com/repeale/fp-go"
	"google.golang.org/protobuf/proto"
)

// EqualProtoContents compares two slices of proto messages to check that the contents are the same.
// It excludes the order and dedupes.
func EqualProtoContents[T proto.Message](a, b []T) bool {
	// We marshal the proto messages to strings so that we can compare them in a set
	marshaledA := fp.Map[T, string](func(x T) string {
		marshal, err := proto.Marshal(x)
		if err != nil {
			panic(err)
		}
		return string(marshal)
	})(a)

	marshaledB := fp.Map[T, string](func(x T) string {
		marshal, err := proto.Marshal(x)
		if err != nil {
			panic(err)
		}
		return string(marshal)
	})(b)

	setA := ToSet[string](marshaledA)
	setB := ToSet[string](marshaledB)
	return setA.Equal(setB)
}

func EqualProtoSlices[T proto.Message](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	for i, x := range a {
		if !proto.Equal(x, b[i]) {
			return false
		}
	}
	return true
}
