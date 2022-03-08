package search

import (
	"fmt"

	"github.com/typesense/typesense-go/typesense"
	"github.com/typesense/typesense-go/typesense/api"
)

type Searcher interface {
	Upsert(ResourceDoc) error
	RunSearch(q string) ([]ResourceDoc, error)
}

type TypeSenseParams struct {
	Host   string
	Port   string
	ApiKey string
}

type Search struct {
	Client *typesense.Client
}

func NewTypesenseSearch(params *TypeSenseParams) (Searcher, error) {
	client := typesense.NewClient(
		typesense.WithServer(fmt.Sprintf("http://%s:%s", params.Host, params.Port)),
		typesense.WithAPIKey(params.ApiKey))
	if _, errSchemaNotFound := client.Collection("resource").Retrieve(); errSchemaNotFound != nil {
		if err := makeSchema(client); err != nil {
			return nil, err
		}
	}

	if errinitcollection := initializeCollection(client); errinitcollection != nil {
		return nil, errinitcollection
	}
	return &Search{
		Client: client,
	}, nil
}

type ResourceDoc struct {
	Name    string
	Variant string
	Type    string
}

func makeSchema(client *typesense.Client) error {
	schema := &api.CollectionSchema{
		Name: "resource",
		Fields: []api.Field{
			{
				Name: "Name",
				Type: "string",
			},
			{
				Name: "Variant",
				Type: "string",
			},
			{
				Name: "Type",
				Type: "string",
			},
		},
	}
	_, err := client.Collections().Create(schema)
	return err
}

func initializeCollection(client *typesense.Client) error {
	var resourceinitial []interface{}
	var resourceempty ResourceDoc
	resourceinitial = append(resourceinitial, resourceempty)
	action := "create"
	batchnum := 40
	params := &api.ImportDocumentsParams{
		Action:    &action,
		BatchSize: &batchnum,
	}
	//initializing resource collection with empty struct so we can use upsert function
	_, err := client.Collection("resource").Documents().Import(resourceinitial, params)
	return err
}

func (s Search) Upsert(doc ResourceDoc) error {
	_, err := s.Client.Collection("resource").Documents().Upsert(doc)
	return err
}

func (s Search) RunSearch(q string) ([]ResourceDoc, error) {
	searchParameters := &api.SearchCollectionParams{
		Q:       q,
		QueryBy: "Name",
	}
	results, errGetResults := s.Client.Collection("resource").Documents().Search(searchParameters)
	if errGetResults != nil {
		return nil, errGetResults
	}
	var searchresults []ResourceDoc
	for _, hit := range *results.Hits {
		doc := *hit.Document
		searchresults = append(searchresults, ResourceDoc{
			Name:    doc["name"].(string),
			Type:    doc["type"].(string),
			Variant: doc["variant"].(string),
		})
	}
	return searchresults, nil
}
