// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"fmt"
	help "github.com/featureform/helpers"
	"net/http"
	"reflect"
	"time"

	"github.com/featureform/metadata"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/typesense/typesense-go/typesense"
	api "github.com/typesense/typesense-go/typesense/api"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var typesenseClient *typesense.Client

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
	Created      time.Time                               `json:"created"`
	Description  string                                  `json:"description"`
	Entity       string                                  `json:"entity"`
	Name         string                                  `json:"name"`
	Owner        string                                  `json:"owner"`
	Provider     string                                  `json:"provider"`
	DataType     string                                  `json:"data-type"`
	Variant      string                                  `json:"variant"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
	Location     map[string]string                       `json:"location"`
	Source       metadata.NameVariant                    `json:"source"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
}

type FeatureResource struct {
	AllVariants    []string                          `json:"all-variants"`
	Type           string                            `json:"type"`
	DefaultVariant string                            `json:"default-variant"`
	Name           string                            `json:"name"`
	Variants       map[string]FeatureVariantResource `json:"variants"`
}

type TrainingSetVariantResource struct {
	Created     time.Time                           `json:"created"`
	Description string                              `json:"description"`
	Name        string                              `json:"name"`
	Owner       string                              `json:"owner"`
	Provider    string                              `json:"provider"`
	Variant     string                              `json:"variant"`
	Label       metadata.NameVariant                `json:"label"`
	Features    map[string][]FeatureVariantResource `json:"features"`
	Status      string                              `json:"status"`
	Error       string                              `json:"error"`
}

type TrainingSetResource struct {
	AllVariants    []string                              `json:"all-variants"`
	Type           string                                `json:"type"`
	DefaultVariant string                                `json:"default-variant"`
	Name           string                                `json:"name"`
	Variants       map[string]TrainingSetVariantResource `json:"variants"`
}

type SourceVariantResource struct {
	Created      time.Time                               `json:"created"`
	Description  string                                  `json:"description"`
	Name         string                                  `json:"name"`
	SourceType   string                                  `json:"source-type"`
	Owner        string                                  `json:"owner"`
	Provider     string                                  `json:"provider"`
	Variant      string                                  `json:"variant"`
	Labels       map[string][]LabelVariantResource       `json:"labels"`
	Features     map[string][]FeatureVariantResource     `json:"features"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
	Definition   string                                  `json:"definition"`
}

type SourceResource struct {
	AllVariants    []string                         `json:"all-variants"`
	Type           string                           `json:"type"`
	DefaultVariant string                           `json:"default-variant"`
	Name           string                           `json:"name"`
	Variants       map[string]SourceVariantResource `json:"variants"`
}

type LabelVariantResource struct {
	Created      time.Time                               `json:"created"`
	Description  string                                  `json:"description"`
	Entity       string                                  `json:"entity"`
	Name         string                                  `json:"name"`
	Owner        string                                  `json:"owner"`
	Provider     string                                  `json:"provider"`
	DataType     string                                  `json:"data-type"`
	Variant      string                                  `json:"variant"`
	Location     map[string]string                       `json:"location"`
	Source       metadata.NameVariant                    `json:"source"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
}

type LabelResource struct {
	AllVariants    []string                        `json:"all-variants"`
	Type           string                          `json:"type"`
	DefaultVariant string                          `json:"default-variant"`
	Name           string                          `json:"name"`
	Variants       map[string]LabelVariantResource `json:"variants"`
}

type EntityResource struct {
	Name         string                                  `json:"name"`
	Type         string                                  `json:"type"`
	Description  string                                  `json:"description"`
	Features     map[string][]FeatureVariantResource     `json:"features"`
	Labels       map[string][]LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
}

type UserResource struct {
	Name         string                                  `json:"name"`
	Type         string                                  `json:"type"`
	Features     map[string][]FeatureVariantResource     `json:"features"`
	Labels       map[string][]LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Sources      map[string][]SourceVariantResource      `json:"sources"`
	Status       string                                  `json:"status"`
}

type ModelResource struct {
	Name         string                                  `json:"name"`
	Type         string                                  `json:"type"`
	Description  string                                  `json:"description"`
	Features     map[string][]FeatureVariantResource     `json:"features"`
	Labels       map[string][]LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
}

type ProviderResource struct {
	Name         string                                  `json:"name"`
	Type         string                                  `json:"type"`
	Description  string                                  `json:"description"`
	ProviderType string                                  `json:"provider-type"`
	Software     string                                  `json:"software"`
	Team         string                                  `json:"team"`
	Sources      map[string][]SourceVariantResource      `json:"sources"`
	Features     map[string][]FeatureVariantResource     `json:"features"`
	Labels       map[string][]LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
	Status       string                                  `json:"status"`
	Error        string                                  `json:"error"`
}

type FetchError struct {
	StatusCode int
	Type       string
}

func (m *FetchError) Error() string {
	return fmt.Sprintf("Error %d: Failed to fetch %s", m.StatusCode, m.Type)
}

func columnsToMap(columns metadata.ResourceVariantColumns) map[string]string {
	columnNameValues := reflect.ValueOf(columns)
	featureColumns := make(map[string]string)
	for i := 0; i < columnNameValues.NumField(); i++ {
		featureColumns[columnNameValues.Type().Field(i).Name] = fmt.Sprintf("%v", columnNameValues.Field(i).Interface())
	}
	return featureColumns
}

func featureShallowMap(variant *metadata.FeatureVariant) FeatureVariantResource {
	return FeatureVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Entity:      variant.Entity(),
		Name:        variant.Name(),
		DataType:    variant.Type(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Source:      variant.Source(),
		Location:    columnsToMap(variant.LocationColumns().(metadata.ResourceVariantColumns)),
		Status:      variant.Status().String(),
		Error:       variant.Error(),
	}
}

func labelShallowMap(variant *metadata.LabelVariant) LabelVariantResource {
	return LabelVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Entity:      variant.Entity(),
		Name:        variant.Name(),
		DataType:    variant.Type(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Source:      variant.Source(),
		Location:    columnsToMap(variant.LocationColumns().(metadata.ResourceVariantColumns)),
		Status:      variant.Status().String(),
		Error:       variant.Error(),
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
		Status:      variant.Status().String(),
		Error:       variant.Error(),
	}
}

func sourceShallowMap(variant *metadata.SourceVariant) SourceVariantResource {
	var sourceType string
	var sourceString string
	if variant.PrimaryDataSQLTableName() == "" {
		sourceType = "Transformation"
		sourceString = variant.SQLTransformationQuery()
	} else {
		sourceType = "Primary Table"
		sourceString = variant.PrimaryDataSQLTableName()
	}
	return SourceVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Name:        variant.Name(),
		SourceType:  sourceType,
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Status:      variant.Status().String(),
		Error:       variant.Error(),
		Definition:  sourceString,
	}
}

func (m *MetadataServer) getTrainingSets(nameVariants []metadata.NameVariant) (map[string][]TrainingSetVariantResource, error) {
	trainingSetMap := make(map[string][]TrainingSetVariantResource)
	trainingSetVariants, err := m.client.GetTrainingSetVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range trainingSetVariants {
		if _, has := trainingSetMap[variant.Name()]; !has {
			trainingSetMap[variant.Name()] = []TrainingSetVariantResource{}
		}
		trainingSetMap[variant.Name()] = append(trainingSetMap[variant.Name()], trainingSetShallowMap(variant))
	}
	return trainingSetMap, nil
}

func (m *MetadataServer) getFeatures(nameVariants []metadata.NameVariant) (map[string][]FeatureVariantResource, error) {
	featureMap := make(map[string][]FeatureVariantResource)
	featureVariants, err := m.client.GetFeatureVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range featureVariants {
		if _, has := featureMap[variant.Name()]; !has {
			featureMap[variant.Name()] = []FeatureVariantResource{}
		}
		featureMap[variant.Name()] = append(featureMap[variant.Name()], featureShallowMap(variant))
	}
	return featureMap, nil
}

func (m *MetadataServer) getLabels(nameVariants []metadata.NameVariant) (map[string][]LabelVariantResource, error) {
	labelMap := make(map[string][]LabelVariantResource)
	labelVariants, err := m.client.GetLabelVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range labelVariants {
		if _, has := labelMap[variant.Name()]; !has {
			labelMap[variant.Name()] = []LabelVariantResource{}
		}
		labelMap[variant.Name()] = append(labelMap[variant.Name()], labelShallowMap(variant))
	}
	return labelMap, nil
}

func (m *MetadataServer) getSources(nameVariants []metadata.NameVariant) (map[string][]SourceVariantResource, error) {
	sourceMap := make(map[string][]SourceVariantResource)
	sourceVariants, err := m.client.GetSourceVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range sourceVariants {
		if _, has := sourceMap[variant.Name()]; !has {
			sourceMap[variant.Name()] = []SourceVariantResource{}
		}
		sourceMap[variant.Name()] = append(sourceMap[variant.Name()], sourceShallowMap(variant))
	}
	return sourceMap, nil
}

func (m *MetadataServer) readFromFeature(feature *metadata.Feature, deepCopy bool) (map[string]FeatureVariantResource, *FetchError) {
	variantMap := make(map[string]FeatureVariantResource)
	variants, err := feature.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "feature variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		featResource := featureShallowMap(variant)
		if deepCopy {
			ts, err := m.getTrainingSets(variant.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal Error", err)
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from training sets"}
			}
			featResource.TrainingSets = ts
		}
		variantMap[variant.Variant()] = featResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromTrainingSet(trainingSet *metadata.TrainingSet, deepCopy bool) (map[string]TrainingSetVariantResource, *FetchError) {
	variantMap := make(map[string]TrainingSetVariantResource)
	variants, err := trainingSet.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "training set variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		trainingResource := trainingSetShallowMap(variant)
		if deepCopy {
			f, err := m.getFeatures(variant.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal Error", err)
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from features"}
			}
			trainingResource.Features = f
		}
		variantMap[variant.Variant()] = trainingResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromSource(source *metadata.Source, deepCopy bool) (map[string]SourceVariantResource, *FetchError) {
	variantMap := make(map[string]SourceVariantResource)
	variants, err := source.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "source variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		sourceResource := sourceShallowMap(variant)
		if deepCopy {
			fetchGroup := new(errgroup.Group)
			fetchGroup.Go(func() error {
				f, err := m.getFeatures(variant.Features())
				if err != nil {
					m.logger.Errorw(err.Error(), "Internal Error", err)
					return &FetchError{StatusCode: 500, Type: "Cannot get information from features"}
				}
				sourceResource.Features = f
				return nil
			})
			fetchGroup.Go(func() error {
				l, err := m.getLabels(variant.Labels())
				if err != nil {
					m.logger.Errorw(err.Error(), "Internal Error", err)
					return &FetchError{StatusCode: 500, Type: "Cannot get information from labels"}
				}
				sourceResource.Labels = l
				return nil
			})
			fetchGroup.Go(func() error {
				ts, err := m.getTrainingSets(variant.TrainingSets())
				if err != nil {
					m.logger.Errorw(err.Error(), "Internal Error", err)
					return &FetchError{StatusCode: 500, Type: "Cannot get information from training sets"}
				}
				sourceResource.TrainingSets = ts
				return nil
			})
			if err := fetchGroup.Wait(); err != nil {
				return nil, err.(*FetchError)
			}
		}
		variantMap[variant.Variant()] = sourceResource

	}
	return variantMap, nil
}

func (m *MetadataServer) readFromLabel(label *metadata.Label, deepCopy bool) (map[string]LabelVariantResource, *FetchError) {
	variantMap := make(map[string]LabelVariantResource)
	variants, err := label.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "label variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {
		labelResource := labelShallowMap(variant)
		if deepCopy {
			ts, err := m.getTrainingSets(variant.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal Error", err)
				return nil, &FetchError{StatusCode: 500, Type: "Cannot get information from training sets"}
			}
			labelResource.TrainingSets = ts
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
	case "sources":
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
			Type:           "Source",
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
		entityResource := EntityResource{
			Name:        entity.Name(),
			Type:        "Entity",
			Description: entity.Description(),
			Status:      entity.Status().String(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(entity.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "feature"}
			}
			entityResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(entity.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "label"}
			}
			entityResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(entity.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "training set"}
			}
			entityResource.TrainingSets = ts
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, entityResource)
	case "users":
		user, err := m.client.GetUser(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "user"}
			m.logger.Errorw(err.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		userResource := &UserResource{
			Name:   user.Name(),
			Type:   "User",
			Status: user.Status().String(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(user.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "feature"}
			}
			userResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(user.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "label"}
			}
			userResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(user.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "training set"}
			}
			userResource.TrainingSets = ts
			return nil
		})
		fetchGroup.Go(func() error {
			s, err := m.getSources(user.Sources())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "source"}
			}
			userResource.Sources = s
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, userResource)
	case "models":
		model, err := m.client.GetModel(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "model"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		modelResource := &ModelResource{
			Name:        model.Name(),
			Type:        "Model",
			Description: model.Description(),
			Status:      model.Status().String(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(model.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "feature"}
			}
			modelResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(model.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "label"}
			}
			modelResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(model.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "training set"}
			}
			modelResource.TrainingSets = ts
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, modelResource)
	case "providers":
		provider, err := m.client.GetProvider(context.Background(), c.Param("resource"))
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "provider"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		providerResource := &ProviderResource{
			Name:         provider.Name(),
			Type:         "Provider",
			Description:  provider.Description(),
			ProviderType: provider.Type(),
			Software:     provider.Software(),
			Team:         provider.Team(),
			Status:       provider.Status().String(),
			Error:        provider.Error(),
		}
		fetchGroup := new(errgroup.Group)
		fetchGroup.Go(func() error {
			f, err := m.getFeatures(provider.Features())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "feature"}
			}
			providerResource.Features = f
			return nil
		})
		fetchGroup.Go(func() error {
			l, err := m.getLabels(provider.Labels())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "label"}
			}
			providerResource.Labels = l
			return nil
		})
		fetchGroup.Go(func() error {
			ts, err := m.getTrainingSets(provider.TrainingSets())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "training set"}
			}
			providerResource.TrainingSets = ts
			return nil
		})
		fetchGroup.Go(func() error {
			s, err := m.getSources(provider.Sources())
			if err != nil {
				m.logger.Errorw(err.Error(), "Internal error", err)
				return &FetchError{StatusCode: 500, Type: "source"}
			}
			providerResource.Sources = s
			return nil
		})
		if err := fetchGroup.Wait(); err != nil {
			c.JSON(err.(*FetchError).StatusCode, err.Error())
		}
		c.JSON(http.StatusOK, providerResource)
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
			variantList, fetchError := m.readFromSource(source, false)
			if fetchError != nil {
				m.logger.Errorw(fetchError.Error())
				c.JSON(fetchError.StatusCode, fetchError.Error())
				return
			}
			sourceList[i] = SourceResource{
				AllVariants:    source.Variants(),
				Type:           "Source",
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
				Status:      entity.Status().String(),
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
				Status:      model.Status().String(),
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
				Name:   user.Name(),
				Type:   "User",
				Status: user.Status().String(),
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
				Status:       provider.Status().String(),
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

func (m *MetadataServer) GetSearch(c *gin.Context) {
	query := c.Param("query")
	searchParameters := &api.SearchCollectionParams{
		Q:       query,
		QueryBy: "Name",
	}
	result, err := typesenseClient.Collection("resource").Documents().Search(searchParameters)
	if err != nil {
		m.logger.Errorw("Failed to fetch resources", "error", err)
		c.JSON(500, "Failed to fetch resources")
		return
	}
	c.JSON(200, result)
}

func (m *MetadataServer) Start(port string) {
	router := gin.Default()
	router.Use(cors.Default())

	router.GET("/data/:type", m.GetMetadataList)
	router.GET("/data/:type/:resource", m.GetMetadata)
	router.GET("/search/:query", m.GetSearch)

	router.Run(port)
}

func main() {
	logger := zap.NewExample().Sugar()
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	typesenseHost := help.GetEnv("TYPESENSE_HOST", "localhost")
	typesensePort := help.GetEnv("TYPESENSE_PORT", "8108")
	typesenseEndpoint := fmt.Sprintf("http://%s:%s", typesenseHost, typesensePort)
	typesenseApiKey := help.GetEnv("TYPESENSE_APIKEY", "xyz")
	logger.Infof("Connecting to typesense at: %s\n", typesenseEndpoint)
	typesenseClient = typesense.NewClient(
		typesense.WithServer(typesenseEndpoint),
		typesense.WithAPIKey(typesenseApiKey))
	metadataAddress := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	logger.Infof("Looking for metadata at: %s\n", metadataAddress)
	client, err := metadata.NewClient(metadataAddress, logger)
	if err != nil {
		logger.Panicw("Failed to connect", "error", err)
	}

	metadata_server, err := NewMetadataServer(logger, client)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}
	metadataHTTPPort := help.GetEnv("METADATA_HTTP_PORT", "3001")
	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	logger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)
	metadata_server.Start(metadataServingPort)
}
