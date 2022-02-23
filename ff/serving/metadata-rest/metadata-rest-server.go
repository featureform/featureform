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
	client *metadata.Client
	logger *zap.SugaredLogger
}

func NewMetadataServer(logger *zap.SugaredLogger, client *metadata.Client) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	return &MetadataServer{
		client: client,
		logger: logger,
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
	AllVariants    []string                           `json:"all-versions"`
	DefaultVariant string                             `json:"default-variant"`
	Name           string                             `json:"name"`
	Variants       *map[string]FeatureVariantResource `json:"versions"`
}

type TrainingSetVariantResource struct {
	Created     time.Time     `json:"created"`
	Description string        `json:"description"`
	Name        string        `json:"name"`
	Owner       string        `json:"owner"`
	Provider    string        `json:"provider"`
	Variant     string        `json:"variant"`
	Label       NameVariant   `json:"label"`
	Features    []NameVariant `json:"features"`
}

type TrainingSetResource struct {
	AllVariants    []string                           `json:"all-versions"`
	DefaultVariant string                             `json:"default-variant"`
	Name           string                             `json:"name"`
	Variants       *map[string]FeatureVariantResource `json:"versions"`
}

type SourceVariantResource struct {
	Created      time.Time     `json:"created"`
	Description  string        `json:"description"`
	Name         string        `json:"name"`
	Type         string        `json:"type"`
	Owner        string        `json:"owner"`
	Provider     string        `json:"provider"`
	Variant      string        `json:"variant"`
	Label        []NameVariant `json:"labels"`
	Features     []NameVariant `json:"features"`
	TrainingSets []NameVariant `json:"trainingsets"`
}

type SourceResource struct {
	AllVariants    []string                          `json:"all-versions"`
	DefaultVariant string                            `json:"default-variant"`
	Name           string                            `json:"name"`
	Variants       *map[string]SourceVariantResource `json:"versions"`
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
	AllVariants    []string                         `json:"all-versions"`
	DefaultVariant string                           `json:"default-variant"`
	Name           string                           `json:"name"`
	Variants       *map[string]LabelVariantResource `json:"versions"`
}

type EntityResource struct {
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	Features     []NameVariant `json:"features"`
	Labels       []NameVariant `json:"labels"`
	TrainingSets []NameVariant `json:"trainingsets"`
}

type UserResource struct {
	Name         string        `json:"name"`
	Features     []NameVariant `json:"features"`
	Labels       []NameVariant `json:"labels"`
	TrainingSets []NameVariant `json:"trainingsets"`
	Sources      []NameVariant `json:"sources"`
}

type ModelResource struct {
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	Features     []NameVariant `json:"features"`
	Labels       []NameVariant `json:"labels"`
	TrainingSets []NameVariant `json:"trainingsets"`
}

type ProviderResource struct {
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	Type         string        `json:"type"`
	Software     string        `json:"software"`
	Team         string        `json:"team"`
	Sources      []NameVariant `json:"sources"`
	Features     []NameVariant `json:"features"`
	Labels       []NameVariant `json:"labels"`
	TrainingSets []NameVariant `json:"trainingsets"`
}

// type FetchError struct{
// 	StatusCode int
// 	Err error
// 	Message string
// }

// func (m *FetchError) Error() string {
// 	return fmt.Sprintf("status %d: err %v\n%v", r.StatusCode, r.Err, r.Message)
// }

func (m MetadataServer) readFromFeature(feature *metadata.Feature) (map[string]FeatureVariantResource, err) {
	variantMap := make(map[string]FeatureVariantResource)
	variants, err := feature.FetchVariants(m.client, context.Background())
	if err != nil {
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, err
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
	return variantMap, nil
}

func (m MetadataServer) readFromTrainingSet(trainingSet *metadata.TrainingSet) (map[string]TrainingSetVariantResource, err) {
	variantMap := make(map[string]TrainingSetVariantResource)
	variants, err := trainingSet.FetchVariants(m.client, context.Background())
	if err != nil {
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, err
	}
	for _, variant := range variants {
		variantMap[variant.Name()] = TrainingSetVariantResource{
			Created:     variant.Created(),
			Description: variant.Description(),
			Name:        variant.Name(),
			Variant:     variant.Name(),
			Owner:       variant.Owner(),
			Provider:    variant.Provider(),
			Label: NameVariant{
				Name:    variant.Label().Name,
				Variant: variant.Label().Variant,
			},
		}
	}
	return variantMap, nil
}

func (m MetadataServer) readFromSource(source *metadata.Source) (map[string]SourceVariantResource, err) {
	variantMap := make(map[string]SourceVariantResource)
	variants, err := source.FetchVariants(m.client, context.Background())
	if err != nil {
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, err
	}
	for _, variant := range variants {
		variantMap[variant.Name()] = SourceVariantResource{
			Created:     variant.Created(),
			Description: variant.Description(),
			Name:        variant.Name(),
			Type:        variant.Type(),
			Variant:     variant.Name(),
			Owner:       variant.Owner(),
			Provider:    variant.Provider(),
		}
	}
	return variantMap, nil
}

func (m MetadataServer) readFromLabel(label *metadata.Label) (map[string]LabelVariantResource, err) {
	variantMap := make(map[string]LabelVariantResource)
	variants, err := label.FetchVariants(m.client, context.Background())
	if err != nil {
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, err
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
	return variantMap, nil
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
			variantList, err := m.readFromFeature(feature)
			if err != nil {
				m.logger.Errorw("Failed to fetch variants", "Error", err)
				c.JSON(500, gin.H{"Error": "Failed to fetch variants"})
				return
			}
			featureList[i] = FeatureResource{
				AllVariants:    feature.Variants(),
				DefaultVariant: feature.DefaultVariant(),
				Name:           feature.Name(),
				Variants:       variantList,
			}
		}
		c.JSON(http.StatusOK, featureList)
	case "training-sets":
		trainingSets, err := m.client.ListTrainingSets(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch training sets", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch training sets"})
			return
		}
		trainingSetList := make([]TrainingSetResource, len(trainingSets))
		for i, trainingSet := range trainingSets {
			variantList, err := m.readFromTrainingSet(trainingSet)
			if err != nil {
				m.logger.Errorw("Failed to fetch variants", "Error", err)
				c.JSON(500, gin.H{"Error": "Failed to fetch variants"})
				return
			}
			trainingSetList[i] = TrainingSetResource{
				AllVariants:    trainingSet.Variants(),
				DefaultVariant: trainingSet.DefaultVariant(),
				Name:           trainingSet.Name(),
				Variants:       variantList,
			}
		}
		c.JSON(http.StatusOK, trainingSetList)
	case "sources":
		sources, err := m.client.ListSources(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch sources", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch sources"})
			return
		}
		sourceList := make([]SourceResource, len(sources))
		for i, source := range sources {
			variantList, err := m.readFromSource(source)
			if err != nil {
				m.logger.Errorw("Failed to fetch variants", "Error", err)
				c.JSON(500, gin.H{"Error": "Failed to fetch variants"})
				return
			}
			sourceList[i] = SourceResource{
				AllVariants:    source.Variants(),
				DefaultVariant: source.DefaultVariant(),
				Name:           source.Name(),
				Variants:       variantList,
			}
		}
		c.JSON(http.StatusOK, sourceList)
	case "labels":
		labels, err := m.client.ListLabels(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch labels", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch labels"})
			return
		}
		labelList := make([]LabelResource, len(labels))

		for i, label := range labels {
			variantList, err := m.readFromLabel(label)
			if err != nil {
				m.logger.Errorw("Failed to fetch variants", "Error", err)
				c.JSON(500, gin.H{"Error": "Failed to fetch variants"})
				return
			}
			labelList[i] = LabelResource{
				AllVariants:    label.Variants(),
				DefaultVariant: label.DefaultVariant(),
				Name:           label.Name(),
				Variants:       variantList,
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
				Name:        entity.Name(),
				Description: entity.Description(),
			}
		}
		c.JSON(http.StatusOK, entityList)

	case "models":
		models, err := m.client.ListModels(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch models", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch models"})
			return
		}
		modelList := make([]ModelResource, len(models))
		for i, model := range models {
			modelList[i] = ModelResource{
				Name:        model.Name(),
				Description: model.Description(),
			}
		}
		c.JSON(http.StatusOK, modelList)

	case "users":
		users, err := m.client.ListUsers(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch users", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch users"})
			return
		}
		userList := make([]UserResource, len(users))
		for i, user := range users {
			userList[i] = UserResource{
				Name: user.Name(),
			}
		}
		c.JSON(http.StatusOK, userList)

	case "providers":
		providers, err := m.client.ListProviders(context.Background())
		if err != nil {
			m.logger.Errorw("Failed to fetch providers", "Error", err)
			c.JSON(500, gin.H{"Error": "Failed to fetch providers"})
			return
		}
		providerList := make([]ProviderResource, len(providers))
		for i, provider := range providers {
			providerList[i] = ProviderResource{
				Name:        provider.Name(),
				Description: provider.Description(),
				Software:    provider.Software(),
				Team:        provider.Team(),
				Type:        provider.Type(),
			}
		}
		c.JSON(http.StatusOK, providerList)

	default:
		m.logger.Errorw("Not a valid data type", "Error", c.Param("type"))
		c.JSON(400, gin.H{"Error": "Not a valid data type", "Type": c.Param("type")})
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
