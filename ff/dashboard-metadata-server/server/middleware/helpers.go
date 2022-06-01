package middleware

import (
	//remote packages
	"context"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	pb "github.com/Sami1309/go-grpc-server/grpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)



type test struct {
	Name string `json:"name"`
}

type metadata struct {
	VersionName string   `json:"version-name"`
	Dimensions  int32    `json:"dimensions"`
	Created     string   `json:"created"`
	Owner       string   `json:"owner"`
	Visibility  string   `json:"visibility"`
	Revision    string   `json:"revision"`
	Tags        []string `json:"tags"`
	Description string   `json:"description"`
}

type data struct {
}

type version struct {
	Metadata metadata `json:"metadata"`
	Data     data     `json:"data"`
}

type space struct {
	Name           string             `json:"name"`
	DefaultVersion string             `json:"default-version"`
	Type           string             `json:"type"`
	AllVersions    []string           `json:"all-versions"`
	Versions       map[string]version `json:"versions"`
}

type listed_space struct {
	Name           string              `json:"name"`
	DefaultVersion string              `json:"default-version"`
	Type           string              `json:"type"`
	AllVersions    []string            `json:"all-versions"`
	Versions       map[string]metadata `json:"versions"`
}



type_struct, err := GetTypeStruct(type)
	if err != nil {
		c.JSON(400, gin.H{"Error:": "Invalid Type"})
	}

	getResponse, getResponseErr := client.Get(ctx, &pb.GetRequest{Type: type, Name: name})
	if getResponseErr != nil {
		c.JSON(500, gin.H{"Error": "Problem fetching object metadata"})
		return
	}

	type_json, err := convertTypeStruct(type_struct, getResponse.GetMetadata())



func getTypeStruct(type)



func ConvertTypeStruct(type_name string, resp interface{}) interface{} {

	wrk := test{Name: "sammy"}
    switch type_name {
    case "Feature":
	case "Feature_Set":
    case "Training_Set":
    }
    return wrk
}


func (cmp *Company) NewWorker(name string) *worker {
    wrk := worker{Name: name}
    cmp.Workers = append(cmp.Workers, wrk)
    return &wrk
}