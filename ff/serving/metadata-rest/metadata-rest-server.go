package main

import (
	"context"
	"fmt"
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
	Created      time.Time              `json:"created"`
	Description  string                 `json:"description"`
	Entity       string                 `json:"entity"`
	Name         string                 `json:"name"`
	Owner        string                 `json:"owner"`
	Provider     string                 `json:"provider"`
	Type         string                 `json:"type"`
	Variant      string                 `json:"variant"`
	Source       metadata.NameVariant   `json:"source"`
	TrainingSets []metadata.NameVariant `json:"trainingsets"`
}

type FeatureResource struct {
	AllVariants    []string                          `json:"all-versions"`
	DefaultVariant string                            `json:"default-variant"`
	Name           string                            `json:"name"`
	Variants       map[string]FeatureVariantResource `json:"versions"`
}

type TrainingSetVariantResource struct {
	Created     time.Time              `json:"created"`
	Description string                 `json:"description"`
	Name        string                 `json:"name"`
	Owner       string                 `json:"owner"`
	Provider    string                 `json:"provider"`
	Variant     string                 `json:"variant"`
	Label       metadata.NameVariant   `json:"label"`
	Features    []metadata.NameVariant `json:"features"`
}

type TrainingSetResource struct {
	AllVariants    []string                              `json:"all-versions"`
	DefaultVariant string                                `json:"default-variant"`
	Name           string                                `json:"name"`
	Variants       map[string]TrainingSetVariantResource `json:"versions"`
}

type SourceVariantResource struct {
	Created      time.Time              `json:"created"`
	Description  string                 `json:"description"`
	Name         string                 `json:"name"`
	Type         string                 `json:"type"`
	Owner        string                 `json:"owner"`
	Provider     string                 `json:"provider"`
	Variant      string                 `json:"variant"`
	Labels       []metadata.NameVariant `json:"labels"`
	Features     []metadata.NameVariant `json:"features"`
	TrainingSets []metadata.NameVariant `json:"trainingsets"`
}

type SourceResource struct {
	AllVariants    []string                         `json:"all-versions"`
	DefaultVariant string                           `json:"default-variant"`
	Name           string                           `json:"name"`
	Variants       map[string]SourceVariantResource `json:"versions"`
}

type LabelVariantResource struct {
	Created      time.Time              `json:"created"`
	Description  string                 `json:"description"`
	Entity       string                 `json:"entity"`
	Name         string                 `json:"name"`
	Owner        string                 `json:"owner"`
	Provider     string                 `json:"provider"`
	Type         string                 `json:"type"`
	Variant      string                 `json:"variant"`
	Source       metadata.NameVariant   `json:"source"`
	TrainingSets []metadata.NameVariant `json:"trainingsets"`
}

type LabelResource struct {
	AllVariants    []string                        `json:"all-versions"`
	DefaultVariant string                          `json:"default-variant"`
	Name           string                          `json:"name"`
	Variants       map[string]LabelVariantResource `json:"versions"`
}

type EntityResource struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	Features     []metadata.NameVariant `json:"features"`
	Labels       []metadata.NameVariant `json:"labels"`
	TrainingSets []metadata.NameVariant `json:"trainingsets"`
}

type UserResource struct {
	Name         string                 `json:"name"`
	Features     []metadata.NameVariant `json:"features"`
	Labels       []metadata.NameVariant `json:"labels"`
	TrainingSets []metadata.NameVariant `json:"trainingsets"`
	Sources      []metadata.NameVariant `json:"sources"`
}

type ModelResource struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	Features     []metadata.NameVariant `json:"features"`
	Labels       []metadata.NameVariant `json:"labels"`
	TrainingSets []metadata.NameVariant `json:"trainingsets"`
}

type ProviderResource struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	Type         string                 `json:"type"`
	Software     string                 `json:"software"`
	Team         string                 `json:"team"`
	Sources      []metadata.NameVariant `json:"sources"`
	Features     []metadata.NameVariant `json:"features"`
	Labels       []metadata.NameVariant `json:"labels"`
	TrainingSets []metadata.NameVariant `json:"trainingsets"`
}

type FetchError struct {
	StatusCode int
	Type       string
}

func (m *FetchError) Error() string {
	return fmt.Sprintf("Error %d: Failed to fetch %s", m.StatusCode, m.Type)
}

func (m MetadataServer) readFromFeature(feature *metadata.Feature) (map[string]FeatureVariantResource, *FetchError) {
	variantMap := make(map[string]FeatureVariantResource)
	variants, err := feature.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "feature variants"}
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, fetchError
	}
	for _, variant := range variants {

		variantMap[variant.Variant()] = FeatureVariantResource{
			Created:      variant.Created(),
			Description:  variant.Description(),
			Entity:       variant.Entity(),
			Name:         variant.Name(),
			Type:         variant.Type(),
			Variant:      variant.Name(),
			Owner:        variant.Owner(),
			Provider:     variant.Provider(),
			Source:       variant.Source(),
			TrainingSets: variant.TrainingSets(),
		}
	}
	return variantMap, nil
}

func (m MetadataServer) readFromTrainingSet(trainingSet *metadata.TrainingSet) (map[string]TrainingSetVariantResource, *FetchError) {
	variantMap := make(map[string]TrainingSetVariantResource)
	variants, err := trainingSet.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "training set variants"}
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, fetchError
	}
	for _, variant := range variants {
		variantMap[variant.Variant()] = TrainingSetVariantResource{
			Created:     variant.Created(),
			Description: variant.Description(),
			Name:        variant.Name(),
			Variant:     variant.Name(),
			Owner:       variant.Owner(),
			Provider:    variant.Provider(),
			Label:       variant.Label(),
			Features:    variant.Features(),
		}
	}
	return variantMap, nil
}

func (m MetadataServer) readFromSource(source *metadata.Source) (map[string]SourceVariantResource, *FetchError) {
	variantMap := make(map[string]SourceVariantResource)
	variants, err := source.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "source variants"}
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, fetchError
	}
	for _, variant := range variants {
		variantMap[variant.Variant()] = SourceVariantResource{
			Created:      variant.Created(),
			Description:  variant.Description(),
			Name:         variant.Name(),
			Type:         variant.Type(),
			Variant:      variant.Name(),
			Owner:        variant.Owner(),
			Provider:     variant.Provider(),
			Labels:       variant.Labels(),
			Features:     variant.Features(),
			TrainingSets: variant.TrainingSets(),
		}
	}
	return variantMap, nil
}

func (m MetadataServer) readFromLabel(label *metadata.Label) (map[string]LabelVariantResource, *FetchError) {
	variantMap := make(map[string]LabelVariantResource)
	variants, err := label.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "label variants"}
		m.logger.Errorw("Failed to fetch variants", "Error", err)
		return variantMap, fetchError
	}
	for _, variant := range variants {
		variantMap[variant.Variant()] = LabelVariantResource{
			Created:      variant.Created(),
			Description:  variant.Description(),
			Entity:       variant.Entity(),
			Name:         variant.Name(),
			Type:         variant.Type(),
			Variant:      variant.Name(),
			Owner:        variant.Owner(),
			Provider:     variant.Provider(),
			Source:       variant.Source(),
			TrainingSets: variant.TrainingSets(),
		}
	}
	return variantMap, nil
}

func (m MetadataServer) GetMetadata(c *gin.Context) {
	switch c.Param("type") {
	case "features":
		features, err := m.client.GetFeatures(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "feature"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		feature := features[0]
		variantList, fetchError := m.readFromFeature(feature)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, FeatureResource{
			AllVariants:    feature.Variants(),
			DefaultVariant: feature.DefaultVariant(),
			Name:           feature.Name(),
			Variants:       variantList,
		})
	case "labels":
		labels, err := m.client.GetLabels(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "label"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		label := labels[0]
		variantList, fetchError := m.readFromLabel(label)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, LabelResource{
			AllVariants:    label.Variants(),
			DefaultVariant: label.DefaultVariant(),
			Name:           label.Name(),
			Variants:       variantList,
		})
	case "training-sets":
		trainingSets, err := m.client.GetTrainingSets(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "training set"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		trainingSet := trainingSets[0]
		variantList, fetchError := m.readFromTrainingSet(trainingSet)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, TrainingSetResource{
			AllVariants:    trainingSet.Variants(),
			DefaultVariant: trainingSet.DefaultVariant(),
			Name:           trainingSet.Name(),
			Variants:       variantList,
		})
	case "sources":
		sources, err := m.client.GetSources(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "source"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		source := sources[0]
		variantList, fetchError := m.readFromSource(source)
		if fetchError != nil {
			m.logger.Errorw(fetchError.Error())
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, SourceResource{
			AllVariants:    source.Variants(),
			DefaultVariant: source.DefaultVariant(),
			Name:           source.Name(),
			Variants:       variantList,
		})
	case "entities":
		entities, err := m.client.GetEntities(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "entity"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		entity := entities[0]
		c.JSON(http.StatusOK, EntityResource{
			Name:         entity.Name(),
			Description:  entity.Description(),
			Features:     entity.Features(),
			Labels:       entity.Labels(),
			TrainingSets: entity.TrainingSets(),
		})
	case "users":
		users, err := m.client.GetUsers(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "user"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		user := users[0]
		c.JSON(http.StatusOK, UserResource{
			Name:         user.Name(),
			Features:     user.Features(),
			Labels:       user.Labels(),
			TrainingSets: user.TrainingSets(),
			Sources:      user.Sources(),
		})
	case "models":
		models, err := m.client.GetModels(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "model"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		model := models[0]
		c.JSON(http.StatusOK, ModelResource{
			Name:         model.Name(),
			Description:  model.Description(),
			Features:     model.Features(),
			Labels:       model.Labels(),
			TrainingSets: model.TrainingSets(),
		})
	case "providers":
		providers, err := m.client.GetProviders(context.Background(), []string{c.Param("resource")})
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "provider"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		provider := providers[0]
		c.JSON(http.StatusOK, ProviderResource{
			Name:         provider.Name(),
			Description:  provider.Description(),
			Type:         provider.Type(),
			Software:     provider.Software(),
			Team:         provider.Team(),
			Sources:      provider.Sources(),
			Features:     provider.Features(),
			Labels:       provider.Labels(),
			TrainingSets: provider.TrainingSets(),
		})
	}
}

func (m MetadataServer) GetMetadataList(c *gin.Context) {

	switch c.Param("type") {
	case "features":
		features, err := m.client.ListFeatures(context.Background())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "features"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		featureList := make([]FeatureResource, len(features))
		for i, feature := range features {
			variantList, fetchError := m.readFromFeature(feature)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
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
			fetchError := &FetchError{StatusCode: 500, Type: "training sets"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		trainingSetList := make([]TrainingSetResource, len(trainingSets))
		for i, trainingSet := range trainingSets {
			variantList, fetchError := m.readFromTrainingSet(trainingSet)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
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
			fetchError := &FetchError{StatusCode: 500, Type: "sources"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		sourceList := make([]SourceResource, len(sources))
		for i, source := range sources {
			variantList, fetchError := m.readFromSource(source)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
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
			fetchError := &FetchError{StatusCode: 500, Type: "labels"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		labelList := make([]LabelResource, len(labels))

		for i, label := range labels {
			variantList, fetchError := m.readFromLabel(label)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
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
			fetchError := &FetchError{StatusCode: 500, Type: "entities"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
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
			fetchError := &FetchError{StatusCode: 500, Type: "models"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
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
			fetchError := &FetchError{StatusCode: 500, Type: "users"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
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
			fetchError := &FetchError{StatusCode: 500, Type: "providers"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
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
		fetchError := &FetchError{StatusCode: 400, Type: c.Param("type")}
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

}

func (m MetadataServer) Start(port string) {
	router := gin.Default()
	router.Use(cors.Default())

	router.GET("/:type", m.GetMetadataList)
	router.GET("/:type/:resource", m.GetMetadata)

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
	metadata_server.Start(metadata_port)
}
