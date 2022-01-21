package main

import (
	"net/http"
	"encoding/json"
	"io/ioutil"

	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/cors"
	"go.uber.org/zap"
)


type MetaServer interface {
	ExposePort(port string)
	GetMetadata(c *gin.Context)
}

type MetadataServer struct {
	Server *FeatureServer
	param_convert map[string]string
}



func NewMetadataServer(logger *zap.SugaredLogger, serv *FeatureServer) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	Param_convert := map[string]string{
		"features": "Feature",
		"training-datasets": "Training Dataset",
		"entities": "Entity",
		"labels": "Label",
		"models": "Model",
		"data-sources": "Data Source",
		"providers": "Provider",
		"users": "User",
	}
	return &MetadataServer{
		Server: serv,
		param_convert: Param_convert,
	}, nil
}

func (m MetadataServer) GetTrainingMetadata(c *gin.Context) {

	name := c.Param("name")
	version := c.Param("version")

	entry, err := m.Server.Metadata.TrainingSetMetadata(name, version)
	if err != nil {
		c.JSON(500, gin.H{"Error": "Problem fetching metadata"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"Metadata": entry})
}

func (m MetadataServer) GetFeatureMetadata(c *gin.Context) {

	name := c.Param("name")
	version := c.Param("version")

	entry, err := m.Server.Metadata.FeatureMetadata(name, version)
	if err != nil {
		c.JSON(500, gin.H{"Error": "Problem fetching metadata"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"Metadata": entry})
}

func (m MetadataServer) GetMetadataList(c *gin.Context) {

	data_type, err := m.param_convert[c.Param("type")]
	if !err {
		c.JSON(400, gin.H{"Error": "Type does not exist"})
		return
	}

	file, _ := ioutil.ReadFile("testdata/metadata/lists/wine-data.json")

	var result map[string]interface{}
	_ = json.Unmarshal([]byte(file), &result)

	data := result[data_type]

	c.JSON(http.StatusOK, data)
}

func (m MetadataServer) GetMetadata(c *gin.Context) {

	data_type := c.Param("type")
	name := c.Param("name")

	file, err := ioutil.ReadFile("testdata/metadata/" + data_type + "/" + name + ".json")
	if err != nil {
		c.JSON(400, gin.H{"Error": "Object does not exist"})
		return
	}

	var result map[string]interface{}
	_ = json.Unmarshal([]byte(file), &result)

	c.JSON(http.StatusOK, result)
}

func (m MetadataServer) ExposePort(port string) {
	router := gin.Default()
	router.Use(cors.Default())

	router.GET("/:type", m.GetMetadataList)
	router.GET("/:type/:name", m.GetMetadata)

	router.Run(port)
}
