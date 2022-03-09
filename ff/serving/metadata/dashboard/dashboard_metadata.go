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

type FeatureVariantResource struct {
	Created      time.Time             `json:"created"`
	Description  string                `json:"description"`
	Entity       string                `json:"entity"`
	Name         string                `json:"name"`
	Owner        string                `json:"owner"`
	Provider     string                `json:"provider"`
	Type         string                `json:"type"`
	Variant      string                `json:"variant"`
	Source       metadata.NameVariant  `json:"source"`
	TrainingSets []TrainingSetResource `json:"training-sets"`
}

type FeatureResource struct {
	AllVariants    []string                          `json:"all-variants"`
	Type           string                            `json:"type"`
	DefaultVariant string                            `json:"default-variant"`
	Name           string                            `json:"name"`
	Variants       map[string]FeatureVariantResource `json:"variants"`
}

type TrainingSetVariantResource struct {
	Created     time.Time            `json:"created"`
	Description string               `json:"description"`
	Name        string               `json:"name"`
	Owner       string               `json:"owner"`
	Provider    string               `json:"provider"`
	Variant     string               `json:"variant"`
	Label       metadata.NameVariant `json:"label"`
	Features    []FeatureResource    `json:"features"`
}

type TrainingSetResource struct {
	AllVariants    []string                              `json:"all-variants"`
	Type           string                                `json:"type"`
	DefaultVariant string                                `json:"default-variant"`
	Name           string                                `json:"name"`
	Variants       map[string]TrainingSetVariantResource `json:"variants"`
}

type SourceVariantResource struct {
	Created      time.Time             `json:"created"`
	Description  string                `json:"description"`
	Name         string                `json:"name"`
	Type         string                `json:"type"`
	Owner        string                `json:"owner"`
	Provider     string                `json:"provider"`
	Variant      string                `json:"variant"`
	Labels       []LabelResource       `json:"labels"`
	Features     []FeatureResource     `json:"features"`
	TrainingSets []TrainingSetResource `json:"training-sets"`
}

type SourceResource struct {
	AllVariants    []string                         `json:"all-variants"`
	Type           string                           `json:"type"`
	DefaultVariant string                           `json:"default-variant"`
	Name           string                           `json:"name"`
	Variants       map[string]SourceVariantResource `json:"variants"`
}

type LabelVariantResource struct {
	Created      time.Time             `json:"created"`
	Description  string                `json:"description"`
	Entity       string                `json:"entity"`
	Name         string                `json:"name"`
	Owner        string                `json:"owner"`
	Provider     string                `json:"provider"`
	Type         string                `json:"type"`
	Variant      string                `json:"variant"`
	Source       metadata.NameVariant  `json:"source"`
	TrainingSets []TrainingSetResource `json:"training-sets"`
}

type LabelResource struct {
	AllVariants    []string                        `json:"all-variants"`
	Type           string                          `json:"type"`
	DefaultVariant string                          `json:"default-variant"`
	Name           string                          `json:"name"`
	Variants       map[string]LabelVariantResource `json:"variants"`
}

type EntityResource struct {
	Name         string                `json:"name"`
	Type         string                `json:"type"`
	Description  string                `json:"description"`
	Features     []FeatureResource     `json:"features"`
	Labels       []LabelResource       `json:"labels"`
	TrainingSets []TrainingSetResource `json:"training-sets"`
}

type UserResource struct {
	Name         string                `json:"name"`
	Type         string                `json:"type"`
	Features     []FeatureResource     `json:"features"`
	Labels       []LabelResource       `json:"labels"`
	TrainingSets []TrainingSetResource `json:"training-sets"`
	Sources      []SourceResource      `json:"primary-data"`
}

type ModelResource struct {
	Name         string                `json:"name"`
	Type         string                `json:"type"`
	Description  string                `json:"description"`
	Features     []FeatureResource     `json:"features"`
	Labels       []LabelResource       `json:"labels"`
	TrainingSets []TrainingSetResource `json:"trainingsets"`
}

type ProviderResource struct {
	Name         string                `json:"name"`
	Type         string                `json:"type"`
	Description  string                `json:"description"`
	ProviderType string                `json:"provider-type"`
	Software     string                `json:"software"`
	Team         string                `json:"team"`
	Sources      []SourceResource      `json:"primary-data"`
	Features     []FeatureResource     `json:"features"`
	Labels       []LabelResource       `json:"labels"`
	TrainingSets []TrainingSetResource `json:"trainingsets"`
}

type FetchError struct {
	StatusCode int
	Type       string
}

func (m *FetchError) Error() string {
	return fmt.Sprintf("Error %d: Failed to fetch %s", m.StatusCode, m.Type)
}

func featureShallowMap(variant *metadata.FeatureVariant) FeatureVariantResource {
	return FeatureVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Entity:      variant.Entity(),
		Name:        variant.Name(),
		Type:        variant.Type(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Source:      variant.Source(),
	}
}

func labelShallowMap(variant *metadata.LabelVariant) LabelVariantResource {
	return LabelVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Entity:      variant.Entity(),
		Name:        variant.Name(),
		Type:        variant.Type(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Source:      variant.Source(),
	}
}

func trainingSetShallowMap(variant *metadata.TrainingSetVariant) TrainingSetVariantResource {
	return TrainingSetVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Name:        variant.Name(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Label:       variant.Label(),
	}
}

func sourceShallowMap(variant *metadata.SourceVariant) SourceVariantResource {
	return SourceVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Name:        variant.Name(),
		Type:        variant.Type(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
	}
}

func (m *MetadataServer) nameVariantToTrainingSetList(nameVariants []metadata.NameVariant) (*[]TrainingSetResource, error) {
	trainingSetMap := make(map[string]TrainingSetResource)
	trainingSetVariants, err := m.client.GetTrainingSetVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range trainingSetVariants {
		
		if trainingSetResource, has := trainingSetMap[variant.Name()]; has {
			trainingSetResource.AllVariants = append(trainingSetMap[variant.Name()].AllVariants, variant.Variant())
			trainingSetResource.Variants[variant.Variant()] = trainingSetShallowMap(variant)
			trainingSetMap[variant.Name()] = trainingSetResource
		} else {
			trainingSetVariantMap := make(map[string]TrainingSetVariantResource)
			trainingSetVariantMap[variant.Variant()] = trainingSetShallowMap(variant)
			trainingSetMap[variant.Name()] = TrainingSetResource{
				AllVariants:    []string{variant.Variant()},
				Type:           "TrainingSet",
				DefaultVariant: "", //if empty frontend should ignore it
				Name:           variant.Name(),
				Variants:       trainingSetVariantMap,
			}
		}
	}
	var trainingSetList []TrainingSetResource
	for _, value := range trainingSetMap {
		trainingSetList = append(trainingSetList, value)
	}
	return &trainingSetList, nil
}

//TODO basically copy logic as above to these
func (m *MetadataServer) nameVariantToFeatureList(nameVariants []metadata.NameVariant) (*[]FeatureResource, error) {
	featureMap := make(map[string]FeatureResource)
	featureVariants, err := m.client.GetFeatureVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range featureVariants {
		
		if featureResource, has := featureMap[variant.Name()]; has {
			featureResource.AllVariants = append(featureMap[variant.Name()].AllVariants, variant.Variant())
			featureResource.Variants[variant.Variant()] = featureShallowMap(variant)
			featureMap[variant.Name()] = featureResource
		} else {
			featureVariantMap := make(map[string]FeatureVariantResource)
			featureVariantMap[variant.Variant()] = featureShallowMap(variant)
			featureMap[variant.Name()] = FeatureResource{
				AllVariants:    []string{variant.Variant()},
				Type:           "Feature",
				DefaultVariant: "", //if empty frontend should ignore it
				Name:           variant.Name(),
				Variants:       featureVariantMap,
			}
		}
	}
	var featureList []FeatureResource
	for _, value := range featureMap {
		featureList = append(featureList, value)
	}
	return &featureList, nil
}

func (m *MetadataServer) nameVariantToLabelList(nameVariants []metadata.NameVariant) (*[]LabelResource, error) {
	labelMap := make(map[string]LabelResource)
	labelVariants, err := m.client.GetLabelVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range labelVariants {
		
		if labelResource, has := labelMap[variant.Name()]; has {
			labelResource.AllVariants = append(labelMap[variant.Name()].AllVariants, variant.Variant())
			labelResource.Variants[variant.Variant()] = labelShallowMap(variant)
			labelMap[variant.Name()] = labelResource
		} else {
			labelVariantMap := make(map[string]LabelVariantResource)
			labelVariantMap[variant.Variant()] = labelShallowMap(variant)
			labelMap[variant.Name()] = LabelResource{
				AllVariants:    []string{variant.Variant()},
				Type:           "Label",
				DefaultVariant: "", //if empty frontend should ignore it
				Name:           variant.Name(),
				Variants:       labelVariantMap,
			}
		}
	}
	var labelList []LabelResource
	for _, value := range labelMap {
		labelList = append(labelList, value)
	}
	return &labelList, nil
}

func (m *MetadataServer) nameVariantToSourceList(nameVariants []metadata.NameVariant) (*[]SourceResource, error) {
	sourceMap := make(map[string]SourceResource)
	sourceVariants, err := m.client.GetSourceVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range sourceVariants {
		
		if sourceResource, has := sourceMap[variant.Name()]; has {
			sourceResource.AllVariants = append(sourceMap[variant.Name()].AllVariants, variant.Variant())
			sourceResource.Variants[variant.Variant()] = sourceShallowMap(variant)
			sourceMap[variant.Name()] = sourceResource
		} else {
			sourceVariantMap := make(map[string]SourceVariantResource)
			sourceVariantMap[variant.Variant()] = sourceShallowMap(variant)
			sourceMap[variant.Name()] = SourceResource{
				AllVariants:    []string{variant.Variant()},
				Type:           "PrimaryData",
				DefaultVariant: "", //if empty frontend should ignore it
				Name:           variant.Name(),
				Variants:       sourceVariantMap,
			}
		}
	}
	var sourceList []SourceResource
	for _, value := range sourceMap {
		sourceList = append(sourceList, value)
	}
	return &sourceList, nil
}

func (m *MetadataServer) readFromFeature(feature *metadata.Feature, singleResource bool) (map[string]FeatureVariantResource, *FetchError) {
	variantMap := make(map[string]FeatureVariantResource)
	variants, err := feature.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "feature variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		featResource := featureShallowMap(variant)
		if singleResource {
			ts, err := m.nameVariantToTrainingSetList(variant.TrainingSets())
			if err != nil {
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from training sets"}
			}
			featResource.TrainingSets = *ts
		}
		variantMap[variant.Variant()] = featResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromTrainingSet(trainingSet *metadata.TrainingSet, singleResource bool) (map[string]TrainingSetVariantResource, *FetchError) {
	variantMap := make(map[string]TrainingSetVariantResource)
	variants, err := trainingSet.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "training set variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		trainingResource := trainingSetShallowMap(variant)
		if singleResource {
			f, err := m.nameVariantToFeatureList(variant.Features())
			if err != nil {
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from features"}
			}
			trainingResource.Features = *f
		}
		variantMap[variant.Variant()] = trainingResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromSource(source *metadata.Source, singleResource bool) (map[string]SourceVariantResource, *FetchError) {
	variantMap := make(map[string]SourceVariantResource)
	variants, err := source.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "source variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		sourceResource := sourceShallowMap(variant)
		if singleResource {
			//TODO make these channels so they don't wait for each other to finish to send their requests
			f, err := m.nameVariantToFeatureList(variant.Features())
			if err != nil {
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from features"}
			}
			l, err := m.nameVariantToLabelList(variant.Labels())
			if err != nil {
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from labels"}
			}
			ts, err := m.nameVariantToTrainingSetList(variant.TrainingSets())
			if err != nil {
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from training sets"}
			}

			sourceResource.Labels = *l
			sourceResource.Features = *f
			sourceResource.TrainingSets = *ts
		}
		variantMap[variant.Variant()] = sourceResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromLabel(label *metadata.Label, singleResource bool) (map[string]LabelVariantResource, *FetchError) {
	variantMap := make(map[string]LabelVariantResource)
	variants, err := label.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "label variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {
		labelResource := labelShallowMap(variant)
		if singleResource {
			ts, err := m.nameVariantToTrainingSetList(variant.TrainingSets())
			if err != nil {
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from training sets"}
			}
			labelResource.TrainingSets = *ts
		}
		variantMap[variant.Variant()] = labelResource
	}
	return variantMap, nil
}

func (m *MetadataServer) GetMetadata(c *gin.Context) {
	switch c.Param("type") {
	case "features":
		feature, err := m.client.GetFeature(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "feature"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromFeature(feature, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, FeatureResource{
			AllVariants:    feature.Variants(),
			Type:           "Feature",
			DefaultVariant: feature.DefaultVariant(),
			Name:           feature.Name(),
			Variants:       variantList,
		})
	case "labels":
		label, err := m.client.GetLabel(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "label"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromLabel(label, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, LabelResource{
			AllVariants:    label.Variants(),
			Type:           "Label",
			DefaultVariant: label.DefaultVariant(),
			Name:           label.Name(),
			Variants:       variantList,
		})
	case "training-sets":
		trainingSet, err := m.client.GetTrainingSet(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "training set"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromTrainingSet(trainingSet, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, TrainingSetResource{
			AllVariants:    trainingSet.Variants(),
			Type:           "TrainingSet",
			DefaultVariant: trainingSet.DefaultVariant(),
			Name:           trainingSet.Name(),
			Variants:       variantList,
		})
	case "primary-data":
		source, err := m.client.GetSource(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "source"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		variantList, fetchError := m.readFromSource(source, true)
		if fetchError != nil {
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, SourceResource{
			AllVariants:    source.Variants(),
			Type:           "PrimaryData",
			DefaultVariant: source.DefaultVariant(),
			Name:           source.Name(),
			Variants:       variantList,
		})
	case "entities":
		entity, err := m.client.GetEntity(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "entity"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		//TODO make these channels so they don't wait for each other to finish to send their requests
		f, err := m.nameVariantToFeatureList(entity.Features())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "feature"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		l, err := m.nameVariantToLabelList(entity.Labels())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "label"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		ts, err := m.nameVariantToTrainingSetList(entity.TrainingSets())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "training set"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}

		c.JSON(http.StatusOK, EntityResource{
			Name:         entity.Name(),
			Type:         "Entity",
			Description:  entity.Description(),
			Features:     *f,
			Labels:       *l,
			TrainingSets: *ts,
		})
	case "users":
		user, err := m.client.GetUser(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "user"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		//TODO make these channels so they don't wait for each other to finish to send their requests
		f, err := m.nameVariantToFeatureList(user.Features())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "feature"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		l, err := m.nameVariantToLabelList(user.Labels())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "label"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		ts, err := m.nameVariantToTrainingSetList(user.TrainingSets())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "training set"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		s, err := m.nameVariantToSourceList(user.Sources())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "source"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}

		c.JSON(http.StatusOK, UserResource{
			Name:         user.Name(),
			Type:         "User",
			Features:     *f,
			Labels:       *l,
			TrainingSets: *ts,
			Sources:      *s,
		})
	case "models":
		model, err := m.client.GetModel(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "model"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		//TODO make these channels so they don't wait for each other to finish to send their requests
		f, err := m.nameVariantToFeatureList(model.Features())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "feature"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		l, err := m.nameVariantToLabelList(model.Labels())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "label"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		ts, err := m.nameVariantToTrainingSetList(model.TrainingSets())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "training set"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}

		c.JSON(http.StatusOK, ModelResource{
			Name:         model.Name(),
			Type:         "Model",
			Description:  model.Description(),
			Features:     *f,
			Labels:       *l,
			TrainingSets: *ts,
		})
	case "providers":
		provider, err := m.client.GetProvider(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "provider"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		//TODO make these channels so they don't wait for each other to finish to send their requests
		f, err := m.nameVariantToFeatureList(provider.Features())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "feature"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		l, err := m.nameVariantToLabelList(provider.Labels())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "label"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		ts, err := m.nameVariantToTrainingSetList(provider.TrainingSets())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "training set"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		s, err := m.nameVariantToSourceList(provider.Sources())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "source"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		c.JSON(http.StatusOK, ProviderResource{
			Name:         provider.Name(),
			Type:         "Provider",
			Description:  provider.Description(),
			ProviderType: provider.Type(),
			Software:     provider.Software(),
			Team:         provider.Team(),
			Sources:      *s,
			Features:     *f,
			Labels:       *l,
			TrainingSets: *ts,
		})
	}
}

func (m *MetadataServer) GetMetadataList(c *gin.Context) {

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
			variantList, fetchError := m.readFromFeature(feature, false)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
				return
			}
			featureList[i] = FeatureResource{
				AllVariants:    feature.Variants(),
				Type:           "Feature",
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
			variantList, fetchError := m.readFromTrainingSet(trainingSet, false)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
				return
			}
			trainingSetList[i] = TrainingSetResource{
				AllVariants:    trainingSet.Variants(),
				Type:           "TrainingSet",
				DefaultVariant: trainingSet.DefaultVariant(),
				Name:           trainingSet.Name(),
				Variants:       variantList,
			}
		}
		c.JSON(http.StatusOK, trainingSetList)
	case "primary-data":
		sources, err := m.client.ListSources(context.Background())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "sources"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		sourceList := make([]SourceResource, len(sources))
		for i, source := range sources {
			variantList, fetchError := m.readFromSource(source, false)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
				return
			}
			sourceList[i] = SourceResource{
				AllVariants:    source.Variants(),
				Type:           "PrimaryData",
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
			variantList, fetchError := m.readFromLabel(label, false)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
				return
			}
			labelList[i] = LabelResource{
				AllVariants:    label.Variants(),
				Type:           "Label",
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
				Type:        "Entity",
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
				Type:        "Model",
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
				Type: "User",
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
				Name:         provider.Name(),
				Type:         "Provider",
				Description:  provider.Description(),
				Software:     provider.Software(),
				Team:         provider.Team(),
				ProviderType: provider.Type(),
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

func (m *MetadataServer) Start(port string) {
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
	if err != nil {
		logger.Panicw("Failed to create server", "Err", err)
	}
	metadata_port := ":8181"
	metadata_server.Start(metadata_port)
}
