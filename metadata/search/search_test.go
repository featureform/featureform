// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package search

import (
	"testing"

	help "github.com/featureform/helpers"
)

func getPort() string {
	return help.GetEnv("MEILISEARCH_PORT", "7700")
}

func getApikey() string {
	return help.GetEnv("MEILISEARCH_API_KEY", "")
}

func TestFullSearch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	params := MeilisearchParams{
		Host:   "localhost",
		Port:   getPort(),
		ApiKey: getApikey(),
	}
	searcher, err := NewMeilisearch(&params)
	if err != nil {
		t.Fatalf("Failed to Initialize Search %s", err)
	}
		res := ResourceDoc{
		Name:    "name",
		Variant: "default",
		Type:    "string",
		Tags:    []string{"tag1", "tag2"},
	}
		if err := searcher.Upsert(res); err != nil {
		t.Fatalf("Failed to Upsert %s", err)
	}
	if _, err := searcher.RunSearch("name"); err != nil {
		t.Fatalf("Failed to start search %s", err)
	}
	if _, err := searcher.RunSearch("tag1"); err != nil {
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
	params := MeilisearchParams{
		Host:   "localhost",
		Port:   getPort(),
		ApiKey: getApikey(),
	}
	searcher, errSearcher := NewMeilisearch(&params)
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
	if err := searcher.DeleteAll(); err != nil {
		t.Fatalf("Failed to Delete %s", err)
	}
}

func TestOrder(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	params := MeilisearchParams{
		Host:   "localhost",
		Port:   getPort(),
		ApiKey: getApikey(),
	}
	searcher, err := NewMeilisearch(&params)
	if err != nil {
		t.Fatalf("Failed to initialize %s", err)
	}
	if err := searcher.DeleteAll(); err != nil {
		t.Fatalf("Failed to Delete %s", err)
	}
	resources := []ResourceDoc{
		{
			Name:    "heroic",
			Variant: "default",
			Type:    "Feature",
		}, {
			Name:    "wine",
			Variant: "second",
			Type:    "Feature",
		}, {
			Name:    "hero",
			Variant: "default-1",
			Type:    "Trainingset",
		}, {
			Name:    "Hero",
			Variant: "second",
			Type:    "Label",
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
		"hero",
		"Hero",
		"heroic",
		"her o",
	}
	for i, hit := range results {
		if hit.Name != names[i] {
			t.Fatalf("Failed to return correct search\n"+
				"Expected: %s, Got: %s\n", names[i], hit.Name)
		}
	}
	//if err := searcher.DeleteAll(); err != nil {
	//	t.Fatalf("Failed to Delete %s", err)
	//}
}
