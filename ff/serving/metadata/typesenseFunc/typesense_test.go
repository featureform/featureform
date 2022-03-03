package abc

import (
	"github.com/typesense/typesense-go/typesense"
	//"github.com/typesense/typesense-go/typesense/api"
	"testing"
)

// func TestUpsert(t *testing.T) {
// 	s := Search{}
// 	err := s.Upsert("abc", "abc")
// 	if err != nil {
// 		t.Fatalf("Failed to upsert: %s", err)
// 	}
// }

func TestCreateGetTable(t *testing.T) {
	s := Search{
		Client: typesense.NewClient(
			typesense.WithServer("http://localhost:8108"),
			typesense.WithAPIKey("xyz")),
	}
	err := s.CreateGetTable()
	if err != nil {
		t.Fatalf("Failed to Create: %s", err)
	}
	_, err3 := s.Client.Collection("newRes").Retrieve()
	if err3 != nil {
		t.Fatalf("Failed to Get: %s", err)
	}
}

func TestEmptyUploadSearch(t *testing.T) {
	s := Search{
		Client: typesense.NewClient(
			typesense.WithServer("http://localhost:8108"),
			typesense.WithAPIKey("xyz")),
	}
	err := s.UploadSearch()
	if err != nil {
		t.Fatalf("Failed to UploadSearch: without values %s", err)
	}
}

func TestUpsertReal(t *testing.T) {
	s := Search{
		Client: typesense.NewClient(
			typesense.WithServer("http://localhost:8108"),
			typesense.WithAPIKey("xyz")),
	}
	err := s.UpsertReal()
	if err != nil {
		t.Fatalf("Failed to UploadSearch with values: %s", err)
	}
}

// func TestFull(t *testing.T) {
// 	s := Search{}
// 	err := s.Fullsearch()
// 	if err != nil {
// 		t.Fatalf("Failed to full search: %s", err)
// 	}
// }
