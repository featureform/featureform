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
	Variant     string `json:"variant"`     //"default-variant"
	Description string `json:"description"` //"Wine quality training dataset",
	Type        string `json:"type"`        //"Training Dataset"
}

type Resource struct {
	Name string `json:"name"`
	//DefaultVariant string   `json:"default-variant"`
	//AllVersions    []string `json:"all-versions"`
	Versionss map[string]Versions `json:"versions"`
}
type Versions struct {
	Name        string `json:"version-name"`
	Description string `json:"description"`
}

//call import to convert it to jsonl things
func main() {
	//handle error in future

	client := typesense.NewClient(
		typesense.WithServer("http://localhost:8108"),
		typesense.WithAPIKey("xyz"))
	//stringg := "ratings_count"
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
				//Facet: tru),
			},
			{
				Name: "description",
				Type: "string",
				//Facet: true,
			},
			{
				Name: "type",
				Type: "string",
			},
		},
		//DefaultSortingField: &stringg,
	}
	//checkthis
	client.Collections().Create(schema)
	action := "create"
	batchnum := 40
	params := &api.ImportDocumentsParams{
		Action:    &action,
		BatchSize: &batchnum,
	}
	//might need to delete existing collection
	featurejson, _ := os.Open("wine-data.json")
	//defer featurejson.Close()
	byteValue, _ := ioutil.ReadAll(featurejson)
	var result map[string][]Resource
	json.Unmarshal((byteValue), &result)
	//keys := make([]string, 0, len(result))
	var finaldict []interface{}
	for typee := range result {
		var resource ResourceID
		resource.Type = typee
		for _, resourcename := range result[typee] {
			resource.Name = resourcename.Name
			dictofversions := resourcename.Versionss
			for n, versionitem := range dictofversions {
				resource.Description = versionitem.Description
				resource.Variant = n
			}
		}
		//keys = append(keys, typee)
		finaldict = append(finaldict, resource)
	}
	//e, _ := json.Marshal(finaldict)
	client.Collection("featuredata").Documents().Import(finaldict, params)
	//sort := "ratings_count:desc"
	searchParameters := &api.SearchCollectionParams{
		Q:       "wyne",
		QueryBy: "name",
		//SortBy:  &sort,
	}
	l, _ := client.Collection("featuredata").Documents().Search(searchParameters)
	file, _ := json.MarshalIndent(l, "", " ")
	_ = ioutil.WriteFile("test.json", file, 0644)
}
