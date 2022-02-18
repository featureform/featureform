package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/typesense/typesense-go/typesense"
	"github.com/typesense/typesense-go/typesense/api"
)

type ResourceID struct {
	Name        string `json:"name"`
	Variant     string `json:"variant"`
	Description string `json:"description"`
	Type        string `json:"type"`
}

type Resource struct {
	Name    string              `json:"name"`
	Version map[string]Versions `json:"versions"`
}
type Versions struct {
	Name        string `json:"version-name"`
	Description string `json:"description"`
}

func main() {
	client := typesense.NewClient(
		typesense.WithServer("http://localhost:8108"),
		typesense.WithAPIKey("xyz"))
	schema := &api.CollectionSchema{
		Name: "featuredata",
		Fields: []api.Field{
			{
				Name: "name",
				Type: "string",
			},
			{
				Name: "variant",
				Type: "string",
			},
			{
				Name: "description",
				Type: "string",
			},
			{
				Name: "type",
				Type: "string",
			},
		},
	}
	client.Collection("featuredata").Delete()
	client.Collections().Create(schema)
	action := "create"
	batchnum := 40
	params := &api.ImportDocumentsParams{
		Action:    &action,
		BatchSize: &batchnum,
	}
	featurejson, _ := os.Open("wine-data.json")
	byteValue, _ := ioutil.ReadAll(featurejson)
	var unmarshalledjson map[string][]Resource
	json.Unmarshal((byteValue), &unmarshalledjson)
	var finalresourceIDs []interface{}
	for type_ := range unmarshalledjson {
		var resource ResourceID
		resource.Type = type_
		for _, resourcename := range unmarshalledjson[type_] {
			resource.Name = resourcename.Name
			dictofversions := resourcename.Version
			for name, versionmeta := range dictofversions {
				resource.Description = versionmeta.Description
				resource.Variant = name
			}
		}
		finalresourceIDs = append(finalresourceIDs, resource)
	}
	client.Collection("featuredata").Documents().Import(finalresourceIDs, params)
}
