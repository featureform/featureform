package lib

import (
	pb "github.com/featureform/lib/sampleproto"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestEqualProtoContents(t *testing.T) {
	// Test: Identical slices of proto messages should return true
	msg1 := &pb.NameVariant{Name: "John", Variant: "Doe"}
	msg2 := &pb.NameVariant{Name: "John", Variant: "Doe"}

	a := []proto.Message{msg1, msg2}
	b := []proto.Message{msg1, msg2}
	if !EqualProtoContents(a, b) {
		t.Error("Expected true for identical slices, got false")
	}

	// Test: Slices with different ordering should return true (since the function seems to consider sets)
	a = []proto.Message{msg1, msg2}
	b = []proto.Message{msg2, msg1}
	if !EqualProtoContents(a, b) {
		t.Error("Expected true for slices with different ordering, got false")
	}

	// Test: Different slices of proto messages should return false
	msg3 := &pb.NameVariant{Name: "Jane", Variant: "Doe"}
	a = []proto.Message{msg1, msg2}
	b = []proto.Message{msg2, msg3}
	if EqualProtoContents(a, b) {
		t.Error("Expected false for different slices, got true")
	}
}
