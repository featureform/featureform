package search

import (
	"testing"
)

func TestFullSearch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	params := TypeSenseParams{
		Host:   "localhost",
		Port:   "8108",
		ApiKey: "xyz",
	}
	searcher, err := NewTypesenseSearch(&params)
	if err != nil {
		t.Fatalf("Failed to Initialize Search %s", err)
	}
	res := ResourceDoc{
		Name:    "name",
		Variant: "default",
		Type:    "string",
	}
	if err := searcher.Upsert(res); err != nil {
		t.Fatalf("Failed to Upsert %s", err)
	}
	if _, err := searcher.RunSearch("name"); err != nil {
		t.Fatalf("Failed to start search %s", err)
	}
	if err := searcher.DeleteAll(); err != nil {
		t.Fatalf("Failed to reset %s", err)
	}
}

func TestCharacters(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	params := TypeSenseParams{
		Host:   "localhost",
		Port:   "8108",
		ApiKey: "xyz",
	}
	searcher, errSearcher := NewTypesenseSearch(&params)
	if errSearcher != nil {
		t.Fatalf("Failed to initialize %s", errSearcher)
	}
	if err := searcher.DeleteAll(); err != nil {
		t.Fatalf("Failed to Delete %s", err)
	}
	resources := []ResourceDoc{
		{
			Name:    "hero-typesense-film",
			Variant: "default_variant",
			Type:    "string-normal",
		}, {
			Name:    "wine_sonoma_county",
			Variant: "second_variant",
			Type:    "general",
		}, {
			Name:    "hero",
			Variant: "default-shirt",
			Type:    "string",
		}, {
			Name:    "Hero",
			Variant: "second_variant",
			Type:    "Entity",
		}, {
			Name:    "juice-dataset-sonome",
			Variant: "third_variant_backup",
			Type:    "Feature",
		},
	}
	for _, resource := range resources {
		if err := searcher.Upsert(resource); err != nil {
			t.Fatalf("Failed to Upsert %s", err)
		}
	}
	results, errRunsearch := searcher.RunSearch("film")
	if errRunsearch != nil {
		t.Fatalf("Failed to start search %s", errRunsearch)
	}
	namesFilm := []string{
		"hero-typesense-film",
	}
	if len(results) == 0 {
		t.Fatalf("Failed to return any search")
	}
	for i, hit := range results {
		if hit.Name != namesFilm[i] {
			t.Fatalf("Failed to return correct search'film'")
		}
	}
	resultsSonoma, errRunsearch := searcher.RunSearch("sonoma")
	if errRunsearch != nil {
		t.Fatalf("Failed to start search %s", errRunsearch)
	}
	namesSonoma := []string{
		"wine_sonoma_county",
		"juice-dataset-sonome",
	}
	if len(results) == 0 {
		t.Fatalf("Failed to return any search")
	}
	for i, hit := range resultsSonoma {
		if hit.Name != namesSonoma[i] {
			t.Fatalf("Failed to return correct search for 'sonoma'")
		}
	}
}

func TestOrder(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	params := TypeSenseParams{
		Host:   "localhost",
		Port:   "8108",
		ApiKey: "xyz",
	}
	searcher, err := NewTypesenseSearch(&params)
	if err != nil {
		t.Fatalf("Failed to initialize %s", err)
	}
	resources := []ResourceDoc{
		{
			Name:    "heroic",
			Variant: "default",
			Type:    "string",
		}, {
			Name:    "wine",
			Variant: "second",
			Type:    "general",
		}, {
			Name:    "hero",
			Variant: "default-1",
			Type:    "string",
		}, {
			Name:    "Hero",
			Variant: "second",
			Type:    "Entity",
		}, {
			Name:    "her o",
			Variant: "third",
			Type:    "Feature",
		},
	}
	for _, resource := range resources {
		if err := searcher.Upsert(resource); err != nil {
			t.Fatalf("Failed to Upsert %s", err)
		}
	}
	results, errRunsearch := searcher.RunSearch("hero")
	if errRunsearch != nil {
		t.Fatalf("Failed to start search %s", errRunsearch)
	}
	names := []string{
		"Hero",
		"hero",
		"heroic",
	}
	for i, hit := range results {
		if hit.Name != names[i] {
			t.Fatalf("Failed to return correct search")
		}
	}
}
