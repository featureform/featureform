package lib

import (
	"github.com/repeale/fp-go"
	"google.golang.org/protobuf/proto"
)

func EqualProtoContents[T proto.Message](a, b []T) bool {
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
