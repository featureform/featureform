package search

import (
	"github.com/typesense/typesense-go/typesense"
	"github.com/typesense/typesense-go/typesense/api"
	"testing"
)

func TestCreateGetTable(t *testing.T) {
	s := Search{
		Client: typesense.NewClient(
			typesense.WithServer("http://localhost:8108"),
			typesense.WithAPIKey("xyz")),
	}
	s.Client.Collection("resource").Delete()
	err := MakeSchema(s.Client)
	if err != nil {
		t.Fatalf("Failed to Make Schema %s", err)
	}
	err2 := InitializeCollection(s.Client)
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
	searchParameters := &api.SearchCollectionParams{
		Q:       "user",
		QueryBy: "Name",
	}
	_, err := RunSearch(searchParameters, s.Client)
	if err != nil {
		t.Fatalf("Failed to UploadSearch: without values %s", err)
	}
}
