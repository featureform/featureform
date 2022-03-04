package search

import (
	"github.com/typesense/typesense-go/typesense"
	"github.com/typesense/typesense-go/typesense/api"
)

type SearchImpl interface {
	CreateTable(name string, schema string) error
	Upsert(key string, value string) error
	Search(table string, q string) ([]string, error)
}
type Search struct {
	Client *typesense.Client
}

type ResourceID struct {
	Name    string
	Variant string
}

func MakeSchema(client *typesense.Client) error {
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

func InitializeCollection(client *typesense.Client) error {
	var resourceinitial []interface{}
	var resourceempty ResourceID
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

func RunSearch(searchParameters *api.SearchCollectionParams, client *typesense.Client) (*api.SearchResult, error) {
	results, err2 := client.Collection("feat").Documents().Search(searchParameters)
	if err2 != nil {
		return nil, err2
	}
	return results, nil
}
