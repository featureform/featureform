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
	client     *metadata.Client
	logger     *zap.SugaredLogger
}

func NewMetadataServer(logger *zap.SugaredLogger, client *metadata.Client) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	return &MetadataServer{
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
	Variants       *map[string]FeatureVariantResource `json:"versions"`
}

type LabelVariantResource struct {
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

type LabelResource struct {
	AllVariants    []string                              `json:"all-versions"`
	DefaultVariant string                                `json:"default-variant"`
	Name           string                                `json:"name"`
	Variants       *map[string]LabelVariantResource `json:"versions"`
}

type EntityResource struct {
	Name           string                                `json:"name"`
	Description  string        `json:"description"`
	Features []NameVariant `json:"features"`
	Labels []NameVariant `json:"labels"`
	TrainingSets []NameVariant `json:"trainingsets"`
}

func (m MetadataServer) readFromFeature(feature *metadata.Feature) *map[string]FeatureVariantResource {
	variantMap := make(map[string]FeatureVariantResource)
	variants, err := m.client.GetFeatureVariants(context.Background(), feature.NameVariants())
	if err != nil {
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return &variantMap
	}
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
	return &variantMap
}

func (m MetadataServer) readFromLabel(label *metadata.Label) *map[string]LabelVariantResource {
	variantMap := make(map[string]LabelVariantResource)
	variants, err := m.client.GetLabelVariants(context.Background(), label.NameVariants())
	if err != nil {
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return &variantMap
	}
	for _, variant := range variants {
		variantMap[variant.Name()] = LabelVariantResource{
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
	return &variantMap
}

func (m MetadataServer) GetMetadataList(c *gin.Context) {

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
			featureList[i] = FeatureResource{
				AllVariants:    feature.Variants(),
				DefaultVariant: feature.DefaultVariant(),
				Name:           feature.Name(),
				Variants:       m.readFromFeature(feature),
			}
		}
		c.JSON(http.StatusOK, featureList)
	case "labels":
		labels, err := m.client.ListLabels(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch labels", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch labels"})
			return
		}
		labelList := make([]LabelResource, len(labels))
		for i, label := range labels {
			labelList[i] = LabelResource{
				AllVariants:    label.Variants(),
				DefaultVariant: label.DefaultVariant(),
				Name:           label.Name(),
				Variants:       m.readFromLabel(label),
			}
		}
		c.JSON(http.StatusOK, labelList)
	case "entities":
		entities, err := m.client.ListEntities(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch entities", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch entities"})
			return
		}
		entityList := make([]EntityResource, len(entities))
		for i, entity := range entities {
			entityList[i] = EntityResource{
				Name: entity.Name(),
				Description: entity.Description(),
			}
		}
		c.JSON(http.StatusOK, entityList)

	default:
		m.logger.Errorw("Not a valid data type", "Error", c.Param("type"))
		c.JSON(400, gin.H{"Error": "Not a valid data type","Type": c.Param("type")})
		return
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
