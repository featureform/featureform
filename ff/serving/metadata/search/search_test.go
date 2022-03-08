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

func TestFullSearch(t *testing.T) {
	var params TypeSenseParams
	params.Host = "localhost"
	params.Port = "8108"
	params.ApiKey = "xyz"
	searcher, err := NewTypesenseSearch(&params)
	if err != nil {
		t.Fatalf("Failed to Initialize Search %s", err)
	}
	var resourcetoadd ResourceDoc
	resourcetoadd.Name = "name"
	resourcetoadd.Variant = "default"
	resourcetoadd.Type = "string"
	errUpsert := searcher.Upsert(resourcetoadd)
	if errUpsert != nil {
		t.Fatalf("Failed to Upsert %s", errUpsert)
	}
	_, errRunsearch := searcher.RunSearch("name")
	if errRunsearch != nil {
		t.Fatalf("Failed to start search %s", errRunsearch)
	}
}

func TestReset(t *testing.T) {
	var params TypeSenseParams
	params.Host = "localhost"
	params.Port = "8108"
	params.ApiKey = "xyz"
	searcher, err1 := Reset(&params)
	if err1 != nil {
		t.Fatalf("Failed to reset %s", err1)
	}
	var resourcetoadd ResourceDoc
	resourcetoadd.Name = "name"
	resourcetoadd.Variant = "default"
	resourcetoadd.Type = "string"
	errUpsert := searcher.Upsert(resourcetoadd)
	if errUpsert != nil {
		t.Fatalf("Failed to Upsert %s", errUpsert)
	}
	_, errRunsearch := searcher.RunSearch("name")
	if errRunsearch != nil {
		t.Fatalf("Failed to start search %s", errRunsearch)
	}
}

func TestOrder(t *testing.T) {
	var params TypeSenseParams
	params.Host = "localhost"
	params.Port = "8108"
	params.ApiKey = "xyz"
	searcher, err1 := Reset(&params)
	if err1 != nil {
		t.Fatalf("Failed to reset %s", err1)
	}
	var resourcetoadd ResourceDoc
	resourcetoadd.Name = "heroic"
	resourcetoadd.Variant = "default"
	resourcetoadd.Type = "string"
	var resourcetoadd2 ResourceDoc
	resourcetoadd2.Name = "wine"
	resourcetoadd2.Variant = "second"
	resourcetoadd2.Type = "general"
	var resourcetoadd3 ResourceDoc
	resourcetoadd3.Name = "hero"
	resourcetoadd3.Variant = "default-1"
	resourcetoadd3.Type = "string"
	var resourcetoadd4 ResourceDoc
	resourcetoadd4.Name = "Hero"
	resourcetoadd4.Variant = "second"
	resourcetoadd4.Type = "Entity"
	var resourcetoadd5 ResourceDoc
	resourcetoadd5.Name = "her o"
	resourcetoadd5.Variant = "third"
	resourcetoadd5.Type = "Feature"
	errUpsert := searcher.Upsert(resourcetoadd)
	if errUpsert != nil {
		t.Fatalf("Failed to Upsert %s", errUpsert)
	}
	searcher.Upsert(resourcetoadd2)
	searcher.Upsert(resourcetoadd3)
	searcher.Upsert(resourcetoadd4)
	searcher.Upsert(resourcetoadd5)
	results, errRunsearch := searcher.RunSearch("hero")
	if errRunsearch != nil {
		t.Fatalf("Failed to start search %s", errRunsearch)
	}
	names := [3]string{
		"Hero",
		"hero",
		"heroic",
	}
	for i, hit := range results {
		if hit.Name != names[i] {
			t.Fatalf("Failed to get correct order %s", errRunsearch)
		}
	}
}
