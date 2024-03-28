package ffsync

import "testing"

func TestUint64OrderedId(t *testing.T) {
	id1 := Uint64OrderedId(1)
	id2 := Uint64OrderedId(2)
	id3 := Uint64OrderedId(1)

	if !id1.Equals(&id3) {
		t.Errorf("Expected id1 to equal id3")
	}

	if id1.Equals(&id2) {
		t.Errorf("Expected id1 to not equal id2")
	}

	if !id1.Less(&id2) {
		t.Errorf("Expected id1 to be less than id2")
	}

	if id2.Less(&id1) {
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
	generator, err := NewMemoryOrderedIdGenerator()
	if err != nil {
		t.Fatalf("Failed to create memory ID generator: %v", err)
	}

	prevId, err := generator.NextId("testNamespace")
	if err != nil {
		t.Fatalf("Failed to get next id for testNamespace: %v", err)
	}

	diffNamespaceId, err := generator.NextId("diffNamespace")
	if err != nil {
		t.Fatalf("Failed to get next id for diffNamespace: %v", err)
	}

	if !prevId.Equals(diffNamespaceId) {
		t.Errorf("Expected id '%s' to equal id '%s'", prevId, diffNamespaceId)
	}

	for i := 0; i < 10; i++ {
		id, err := generator.NextId("testNamespace")
		if err != nil {
			t.Fatalf("Failed to get next id for testNamespace: %v", err)
		}

		if !prevId.Less(id) {
			t.Errorf("Expected id '%s' to be greater than previous id '%s'", id, prevId)
		}
		prevId = id
	}
}

func TestETCDIdGenerator(t *testing.T) {
	generator, err := NewETCDOrderedIdGenerator()
	if err != nil {
		t.Fatalf("Failed to create ETCD ID generator: %v", err)
	}

	prevId, err := generator.NextId("testNamespace")
	if err != nil {
		t.Fatalf("Failed to get next id: %v", err)
	}

	diffNamespaceId, err := generator.NextId("diffNamespace")
	if err != nil {
		t.Fatalf("Failed to get next id: %v", err)
	}

	if !prevId.Equals(diffNamespaceId) {
		t.Errorf("Expected id '%s' to equal id '%s'", prevId, diffNamespaceId)
	}

	for i := 0; i < 10; i++ {
		id, err := generator.NextId("testNamespace")
		if err != nil {
			t.Fatalf("Failed to get next id: %v", err)
		}

		if !prevId.Less(id) {
			t.Errorf("Expected id '%s' to be greater than previous id '%s'", id, prevId)
		}
		prevId = id
	}
}

func TestRDSIdGenerator(t *testing.T) {
	generator, err := NewRDSOrderedIdGenerator()
	if err != nil {
		t.Fatalf("Failed to create RDS ID generator: %v", err)
	}

	prevId, err := generator.NextId("testNamespace")
	if err != nil {
		t.Fatalf("Failed to get next id: %v", err)
	}

	diffNamespaceId, err := generator.NextId("diffNamespace")
	if err != nil {
		t.Fatalf("Failed to get next id: %v", err)
	}

	if !prevId.Equals(diffNamespaceId) {
		t.Errorf("Expected id '%s' to equal id '%s'", prevId, diffNamespaceId)
	}

	for i := 0; i < 10; i++ {
		id, err := generator.NextId("testNamespace")
		if err != nil {
			t.Fatalf("Failed to get next id: %v", err)
		}

		if !prevId.Less(id) {
			t.Errorf("Expected id '%s' to be greater than previous id '%s'", id, prevId)
		}
		prevId = id
	}
}
