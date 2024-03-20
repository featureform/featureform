package ffsync

import "testing"

func TestUint64OrderedId(t *testing.T) {
	id1 := uint64OrderedId(1)
	id2 := uint64OrderedId(2)
	id3 := uint64OrderedId(1)

	if !id1.Equals(id3) {
		t.Errorf("Expected id1 to equal id3")
	}

	if id1.Equals(id2) {
		t.Errorf("Expected id1 to not equal id2")
	}

	if !id1.Less(id2) {
		t.Errorf("Expected id1 to be less than id2")
	}

	if id2.Less(id1) {
		t.Errorf("Expected id2 to not be less than id1")
	}

	if id1.String() != "1" {
		t.Errorf("Expected id1 to be '1'")
	}

	if id2.String() != "2" {
		t.Errorf("Expected id2 to be '2'")
	}
}

func TestMemoryIdGenerator(t *testing.T) {
	generator := NewMemoryIdGenerator()

	prevId, _ := generator.NextId("testNamespace")
	diffNamespaceId, _ := generator.NextId("diffNamespace")
	if !prevId.Equals(diffNamespaceId) {
		t.Errorf("Expected id '%s' to equal id '%s'", prevId, diffNamespaceId)
	}

	for i := 0; i < 10; i++ {
		id, _ := generator.NextId("testNamespace")
		if !prevId.Less(id) {
			t.Errorf("Expected id '%s' to be greater than previous id '%s'", id, prevId)
		}
		prevId = id
	}
}
