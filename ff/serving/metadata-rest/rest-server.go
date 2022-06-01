package main

import (
	"net/http"
	"encoding/json"
	//"io/ioutil"
	"context"
	"reflect"

	//"strconv"
	//"fmt"

	// pb "github.com/featureform/serving/metadata/proto"
	"github.com/featureform/serving/metadata"
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/cors"
	"go.uber.org/zap"
)



type MetaServer interface {
	ExposePort(port string)
	GetMetadata(c *gin.Context)
}

type MetadataServer struct {
	param_convert map[string]string
	client *metadata.Client
	logger *zap.SugaredLogger
}



func NewMetadataServer(logger *zap.SugaredLogger, client *metadata.Client) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	Param_convert := map[string]string{
		"features": "Feature",
		"training-sets": "Training Dataset",
		"entities": "Entity",
		"labels": "Label",
		"models": "Model",
		"data-sources": "Data Source",
		"providers": "Provider",
		"users": "User",
	}
	return &MetadataServer{
		param_convert: Param_convert,
		client: client,
		logger: logger,
	}, nil
}

// func (m MetadataServer) GetTrainingMetadata(c *gin.Context) {

// 	name := c.Param("name")
// 	version := c.Param("version")

// 	entry, err := m.Server.Metadata.TrainingSetMetadata(name, version)
// 	if err != nil {
// 		c.JSON(500, gin.H{"Error": "Problem fetching metadata"})
// 		return
// 	}
// 	c.JSON(http.StatusOK, gin.H{"Metadata": entry})
// }

// func (m MetadataServer) GetFeatureMetadata(c *gin.Context) {

// 	name := c.Param("name")
// 	version := c.Param("version")

// 	entry, err := m.Server.Metadata.FeatureMetadata(name, version)
// 	if err != nil {
// 		c.JSON(500, gin.H{"Error": "Problem fetching metadata"})
// 		return
// 	}
// 	c.JSON(http.StatusOK, gin.H{"Metadata": entry})
// }

func (m MetadataServer) GetMetadataList(c *gin.Context) {

	data_type, err := m.param_convert[c.Param("type")]
	if !err {
		c.JSON(400, gin.H{"Error": "Type does not exist"})
		return
	}

	switch data_type {
	case "Feature":
		features, err := m.client.ListFeatures(context.Background())
		if err != nil{
			c.JSON(400, gin.H{"Error": "Failed to fetch features"})
			return
		}
		feature_list := make([]map[string]interface{}, reflect.ValueOf(features).Len())
		for i, s := range features {
			var feature map[string]interface{}
			err := json.Unmarshal([]byte(s.String()), &feature)
			if err != nil{
				c.JSON(400, gin.H{"Error": "Error unmarshaling json"})
				return
			}
			feature["all-versions"] = feature["variants"]
			variant_list := reflect.ValueOf(feature["variants"])
			versionNameVariants := make([]metadata.NameVariant, variant_list.Len())
			for i := 0; i < variant_list.Len(); i++ {
				variant_name := variant_list.Index(i).Interface().(string)
				versionNameVariants[i] = metadata.NameVariant{feature["name"].(string),variant_name}
			}
			variants, err := m.client.GetFeatureVariants(context.Background(), versionNameVariants)
			if err != nil{
				c.JSON(400, gin.H{"Error": "Error fetching variants"})
				return
			}
			
			feature["versions"] = map[string]interface{}{}
			for _, s := range variants {
				var variant_details map[string]interface{}
				err := json.Unmarshal([]byte(s.String()), &variant_details)
				if err != nil{
					c.JSON(400, gin.H{"Error": "Error unmarshaling json"})
					return
				}
				feature["versions"].(map[string]interface{})[variant_details["variant"].(string)] = variant_details
			}

			feature["default-variant"] = feature["defaultVariant"]
			feature_list[i] = feature	
		}
		c.JSON(http.StatusOK, feature_list)

	}
	


	


	// var result map[string]interface{}
	// _ = json.Unmarshal([]byte(file), &result)

	// data := result[data_type]

	// c.JSON(http.StatusOK, data)
}

func (m MetadataServer) GetMetadata(c *gin.Context) {

	data_type, err := m.param_convert[c.Param("type")]
	if !err {
		c.JSON(400, gin.H{"Error": "Type does not exist"})
		return
	}
	name := c.Param("name")

	switch data_type {
	case "Feature":
		feature, err := m.client.GetFeatures(context.Background(), []string{name})
		if err != nil {
			c.JSON(400, gin.H{"Error": "Feature does not exist"})
		return
		}
		m.logger.Debug(feature)
		for i, s := range feature {
			//loop through each version and get full data from each version
			//Send each request async and wait until termination (ch?) and then return full json
			m.logger.Debug(i, s.String())
			c.JSON(http.StatusOK, s)
		}
		
	}


	// if err != nil {
	// 	c.JSON(400, gin.H{"Error": "Object does not exist"})
	// 	return
	// }

	// var result map[string]interface{}
	// _ = json.Unmarshal([]byte(file), &result)

	// c.JSON(http.StatusOK, result)
}

func (m MetadataServer) ExposePort(port string) {
	router := gin.Default()
	router.Use(cors.Default())

	router.GET("/:type", m.GetMetadataList)
	router.GET("/:type/:name", m.GetMetadata)

	router.Run(port)
}

func main() {

	logger := zap.NewExample().Sugar()
	client, err := metadata.NewClient("localhost:8080", logger)
	if err != nil {
		logger.Panicw("Failed to connect", "Err", err)
	}

	metadata_server, err := NewMetadataServer(logger, client)
	metadata_port := ":8181"
	metadata_server.ExposePort(metadata_port)
}