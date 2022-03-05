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

//type ResourceType string ask

func NewTypesenseSearch(params *TypeSenseParams) (Searcher, error) {
	client := typesense.NewClient(
		typesense.WithServer(fmt.Sprintf("http://%s:%s", params.Host, params.Port)),
		typesense.WithAPIKey(params.ApiKey))
	_, err3 := client.Collection("resource").Retrieve()
	if err3 != nil {
		errmakeSchema := makeSchema(client)
		if errmakeSchema != nil {
			return nil, errmakeSchema
		}
	}

	errinitcollection := initializeCollection(client)
	if errinitcollection != nil {
		return nil, errinitcollection
	}
	return &Search{
		Client: client,
	}, nil //ask
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
	_, err1 := client.Collections().Create(schema)
	if err1 != nil {
		return err1
	}
	return nil
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
	_, err5 := client.Collection("resource").Documents().Import(resourceinitial, params)
	if err5 != nil {
		return err5
	}
	return nil
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
	results, err2 := s.Client.Collection("resource").Documents().Search(searchParameters)
	if err2 != nil {
		return nil, err2
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
