// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package search

import (
	"fmt"
	"strings"
	"time"

	re "github.com/avast/retry-go/v4"
	ms "github.com/meilisearch/meilisearch-go"
)

type Searcher interface {
	Upsert(ResourceDoc) error
	RunSearch(q string) ([]ResourceDoc, error)
	DeleteAll() error
}

type MeilisearchParams struct {
	Host   string
	Port   string
	ApiKey string
}

type Search struct {
	client *ms.Client
}

func NewMeilisearch(params *MeilisearchParams) (Searcher, error) {
	address := fmt.Sprintf("http://%s:%s", params.Host, params.Port)
	client := ms.NewClient(ms.ClientConfig{
		Host:   address,
		APIKey: params.ApiKey,
	})

	search := Search{
		client: client,
	}

	// Retries connection to meilisearch
	err := healthCheck(client)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}

	if err := search.initializeCollection(); err != nil {
		return nil, fmt.Errorf("could not initialize collection: %v", err)
	}
	return &search, nil
}

func healthCheck(client *ms.Client) error {
	err := re.Do(
		func() error {
			if _, errRetr := client.Health(); errRetr != nil {
				if strings.Contains(errRetr.Error(), "connection refused") {
					fmt.Printf("could not connect to search. retrying...\n")
				} else {
					return re.Unrecoverable(errRetr)
				}
				return errRetr
			}
			return nil
		},
		re.DelayType(func(n uint, err error, config *re.Config) time.Duration {
			return re.BackOffDelay(n, err, config)
		}),
		re.Attempts(10),
	)
	return err
}

type ResourceDoc struct {
	Name    string
	Variant string
	Type    string
	Tags 	[]string
}

func (s Search) waitForSync(taskUID int64) error {
	task, err := s.client.GetTask(taskUID)
	if err != nil {
		return fmt.Errorf("could not get task: %v", err)
	}
	for task.Status != ms.TaskStatusSucceeded {
		task, err = s.client.GetTask(taskUID)
		if err != nil {
			return fmt.Errorf("could not get task: %v", err)
		}
		if task.Status == ms.TaskStatusFailed {
			return fmt.Errorf(task.Error.Code)
		}
	}
	return nil
}

func (s Search) initializeCollection() error {
	resp, err := s.client.CreateIndex(&ms.IndexConfig{
		Uid:        "resources",
		PrimaryKey: "ID",
	})
	if err != nil {
		return fmt.Errorf("index creation request failed: %v", err)
	}

	err = s.waitForSync(resp.TaskUID)
	if err != nil && err.Error() == "index_already_exists" {
		return nil
	} else if err != nil {
		return fmt.Errorf("could not create index: %v", err)
	}

	return nil
}

func (s Search) Upsert(doc ResourceDoc) error {
		document := map[string]interface{}{
		"ID":      strings.ReplaceAll(fmt.Sprintf("%s__%s__%s", doc.Type, doc.Name, doc.Variant), " ", ""),
		"Parsed":  strings.ReplaceAll(fmt.Sprintf("%s__%s__%s", doc.Type, doc.Name, doc.Variant), "_", " "),
		"Name":    doc.Name,
		"Type":    doc.Type,
		"Variant": doc.Variant,
		"Tags":	   doc.Tags,
	}
	resp, err := s.client.Index("resources").UpdateDocuments(document)
	if err != nil {
		return err
	}
	if err := s.waitForSync(resp.TaskUID); err != nil {
		fmt.Printf("Could not Upsert %#v: %v", document, err)
	}
	return nil
}

func (s Search) DeleteAll() error {
	_, err := s.client.DeleteIndex("resources")
	if err != nil {
		return fmt.Errorf("failed to delete index: %v", err)
	}
	return nil
}

func (s Search) RunSearch(q string) ([]ResourceDoc, error) {
		results, err := s.client.Index("resources").Search(q, &ms.SearchRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to search: %v", err)
	}
	
	var searchResults []ResourceDoc
	
	for _, hit := range results.Hits {
				doc := hit.(map[string]interface{})

		var tags []string
		if tagSlice, ok := doc["Tags"].([]interface{}); ok {
			for _, tag := range tagSlice {
				if strTag, ok := tag.(string); ok {
					tags = append(tags, strTag)
				}
			}
		}
		searchResults = append(searchResults, ResourceDoc{
			Name:    doc["Name"].(string),
			Type:    doc["Type"].(string),
			Variant: doc["Variant"].(string),
			Tags:    tags,
		})

	}
	return searchResults, nil
}
