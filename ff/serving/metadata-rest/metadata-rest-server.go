package main

import (
	"context"
	"github.com/featureform/serving/metadata"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type MetadataServer struct {
	validTypes map[string]bool
	client     *metadata.Client
	logger     *zap.SugaredLogger
}

func NewMetadataServer(logger *zap.SugaredLogger, client *metadata.Client) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	validTypes := map[string]bool{
		"features":          true,
		"training-datasets": true,
		"entities":          true,
		"labels":            true,
		"models":            true,
		"data-sources":      true,
		"providers":         true,
		"users":             true,
	}
	return &MetadataServer{
		validTypes: validTypes,
		client:     client,
		logger:     logger,
	}, nil
}

type NameVariant struct {
	Name    string `json:"name"`
	Variant string `json:"variant"`
}

type FeatureVariantResource struct {
	Created      time.Time     `json:"created"`
	Description  string        `json:"description"`
	Entity       string        `json:"entity"`
	Name         string        `json:"name"`
	Owner        string        `json:"owner"`
	Provider     string        `json:"provider"`
	Type         string        `json:"type"`
	Variant      string        `json:"variant"`
	Source       NameVariant   `json:"source"`
	TrainingSets []NameVariant `json:"trainingsets"`
}

type FeatureResource struct {
	AllVariants    []string                              `json:"all-versions"`
	DefaultVariant string                                `json:"default-variant"`
	Name           string                                `json:"name"`
	Variants       map[string]FeatureVariantResourceDeep `json:"versions"`
}



func (m MetadataServer) GetMetadataList(c *gin.Context) {

	dataType, err := m.validTypes[c.Param("type")]
	if !err || dataType == false {
		m.logger.Errorw("Not a valid data type", "Error", err)
		c.JSON(400, gin.H{"Error": "Not a valid data type"})
		return
	}

	switch c.Param("type") {
	case "features":
		features, err := m.client.ListFeatures(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch features", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch features"})
			return
		}
		featureList := make([]FeatureResource, len(features))
		for i, feature := range features {

			variants, err := m.client.GetFeatureVariants(context.Background(), feature.NameVariants())
			if err != nil {
				m.logger.Errorw("Failed to fetch variants", "Error", err)
				c.JSON(500, gin.H{"Error": "Failed to fetch variants"})
				return
			}
			variantMap := make(map[string]FeatureVariantResource)
			for _, variant := range variants {
				variantMap[variant.Name()] = FeatureVariantResource{
					Created:     variant.Created(),
					Description: variant.Description(),
					Entity:      variant.Entity(),
					Name:        variant.Name(),
					Type:        variant.Type(),
					Variant:     variant.Name(),
					Owner:       variant.Owner(),
					Provider:    variant.Provider(),
					Source: NameVariant{
						Name:    variant.Source().Name,
						Variant: variant.Source().Variant,
					},
				}
			}
			featureList[i] = FeatureResource{
				AllVariants:    feature.Variants(),
				DefaultVariant: feature.DefaultVariant(),
				Name:           feature.Name(),
				Variants:       variantMap,
			}
		}
		c.JSON(http.StatusOK, featureList)
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
