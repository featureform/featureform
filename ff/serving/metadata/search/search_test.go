package search

import (
	"github.com/typesense/typesense-go/typesense"
	"testing"
)

func TestCreateGetTable(t *testing.T) {
	s := Search{
		Client: typesense.NewClient(
			typesense.WithServer("http://localhost:8108"),
			typesense.WithAPIKey("xyz")),
	}
	s.Client.Collection("resource").Delete()
	err := makeSchema(s.Client)
	if err != nil {
		t.Fatalf("Failed to Make Schema %s", err)
	}
	err2 := initializeCollection(s.Client)
	if err2 != nil {
		t.Fatalf("Failed to Create Collection: %s", err)
	}
	_, err3 := s.Client.Collection("resource").Retrieve()
	if err3 != nil {
		t.Fatalf("Failed to Get Collection: %s", err)
	}
}

func TestUploadSearch(t *testing.T) {
	s := Search{
		Client: typesense.NewClient(
			typesense.WithServer("http://localhost:8108"),
			typesense.WithAPIKey("xyz")),
	}
	searchParameters := "user"
	_, err := s.RunSearch(searchParameters)
	if err != nil {
		t.Fatalf("Failed to UploadSearch: without values %s", err)
	}
}
