package main

import (
	"context"
	"encoding/json"
	"github.com/featureform/serving/metadata"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"net/http"
	"reflect"
)

type MetaServer interface {
	ExposePort(port string)
	GetMetadata(c *gin.Context)
}

type MetadataServer struct {
	param_convert map[string]string
	client        *metadata.Client
	logger        *zap.SugaredLogger
}

func NewMetadataServer(logger *zap.SugaredLogger, client *metadata.Client) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	Param_convert := map[string]string{
		"features":          "Feature",
		"training-datasets": "Training Dataset",
		"entities":          "Entity",
		"labels":            "Label",
		"models":            "Model",
		"data-sources":      "Data Source",
		"providers":         "Provider",
		"users":             "User",
	}
	return &MetadataServer{
		param_convert: Param_convert,
		client:        client,
		logger:        logger,
	}, nil
}

func (m MetadataServer) GetMetadataList(c *gin.Context) {

	data_type, err := m.param_convert[c.Param("type")]
	if !err {
		c.JSON(400, gin.H{"Error": "Type does not exist"})
		return
	}

	switch data_type {
	case "Feature":
		features, err := m.client.ListFeatures(context.Background())
		if err != nil {
			c.JSON(400, gin.H{"Error": "Failed to fetch features"})
			return
		}
		feature_list := make([]map[string]interface{}, reflect.ValueOf(features).Len())
		for i, s := range features {
			var feature map[string]interface{}
			err := json.Unmarshal([]byte(s.String()), &feature)
			if err != nil {
				c.JSON(400, gin.H{"Error": "Error unmarshaling json"})
				return
			}
			feature["all-versions"] = feature["variants"]
			variant_list := reflect.ValueOf(feature["variants"])
			versionNameVariants := make([]metadata.NameVariant, variant_list.Len())
			for i := 0; i < variant_list.Len(); i++ {
				variant_name := variant_list.Index(i).Interface().(string)
				versionNameVariants[i] = metadata.NameVariant{feature["name"].(string), variant_name}
			}
			variants, err := m.client.GetFeatureVariants(context.Background(), versionNameVariants)
			if err != nil {
				c.JSON(400, gin.H{"Error": "Error fetching variants"})
				return
			}

			feature["versions"] = map[string]interface{}{}
			for _, s := range variants {
				var variant_details map[string]interface{}
				err := json.Unmarshal([]byte(s.String()), &variant_details)
				if err != nil {
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

}

func (m MetadataServer) ExposePort(port string) {
	router := gin.Default()
	router.Use(cors.Default())

	router.GET("/:type", m.GetMetadataList)

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
