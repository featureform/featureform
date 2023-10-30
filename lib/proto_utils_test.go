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

func TestEqualProtoSlices(t *testing.T) {
	type args[T proto.Message] struct {
		a []T
		b []T
	}
	type testCase[T proto.Message] struct {
		name string
		args args[T]
		want bool
	}

	// Set up the messages for the test cases with significantly different names
	msg1 := &pb.NameVariant{Name: "Alice", Variant: "Smith"}
	msg2 := &pb.NameVariant{Name: "Bob", Variant: "Jones"}
	msg3 := &pb.NameVariant{Name: "Charlie", Variant: "Brown"}

	tests := []testCase[proto.Message]{
		{
			name: "Identical slices of proto messages should return true",
			args: args[proto.Message]{a: []proto.Message{msg1, msg2}, b: []proto.Message{msg1, msg2}},
			want: true,
		},
		{
			name: "Slices with different ordering should return false",
			args: args[proto.Message]{a: []proto.Message{msg1, msg2}, b: []proto.Message{msg2, msg1}},
			want: false,
		},
		{
			name: "Different slices of proto messages should return false",
			args: args[proto.Message]{a: []proto.Message{msg1, msg2}, b: []proto.Message{msg2, msg3}},
			want: false,
		},
		{
			name: "Slices of different lengths should return false",
			args: args[proto.Message]{a: []proto.Message{msg1, msg2}, b: []proto.Message{msg1}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EqualProtoSlices(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("EqualProtoSlices() = %v, want %v", got, tt.want)
			}
		})
	}
}
