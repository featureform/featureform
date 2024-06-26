// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	filestore "github.com/featureform/filestore"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/metadata/search"
	"github.com/featureform/proto"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/serving"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var SearchClient search.Searcher

type StorageProvider interface {
	GetResourceLookup() (metadata.ResourceLookup, error)
}

type LocalStorageProvider struct {
}

func (sp LocalStorageProvider) GetResourceLookup() (metadata.ResourceLookup, error) {
	lookup := make(metadata.LocalResourceLookup)
	return lookup, nil
}

type MetadataServer struct {
	lookup          metadata.ResourceLookup
	client          *metadata.Client
	logger          *zap.SugaredLogger
	StorageProvider StorageProvider
}

func NewMetadataServer(logger *zap.SugaredLogger, client *metadata.Client, storageProvider *metadata.EtcdStorageProvider) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	lookup, err := storageProvider.GetResourceLookup()
	if err != nil {
		return nil, fmt.Errorf("could not configure storage provider: %v", err)
	}
	return &MetadataServer{
		client:          client,
		logger:          logger,
		StorageProvider: storageProvider,
		lookup:          lookup,
	}, nil
}

type FeatureResource struct {
	AllVariants    []string                                   `json:"all-variants"`
	Type           string                                     `json:"type"`
	DefaultVariant string                                     `json:"default-variant"`
	Name           string                                     `json:"name"`
	Variants       map[string]metadata.FeatureVariantResource `json:"variants"`
}

type TrainingSetResource struct {
	AllVariants    []string                                       `json:"all-variants"`
	Type           string                                         `json:"type"`
	DefaultVariant string                                         `json:"default-variant"`
	Name           string                                         `json:"name"`
	Variants       map[string]metadata.TrainingSetVariantResource `json:"variants"`
}

type SourceResource struct {
	AllVariants    []string                                  `json:"all-variants"`
	Type           string                                    `json:"type"`
	DefaultVariant string                                    `json:"default-variant"`
	Name           string                                    `json:"name"`
	Variants       map[string]metadata.SourceVariantResource `json:"variants"`
}

type LabelResource struct {
	AllVariants    []string                                 `json:"all-variants"`
	Type           string                                   `json:"type"`
	DefaultVariant string                                   `json:"default-variant"`
	Name           string                                   `json:"name"`
	Variants       map[string]metadata.LabelVariantResource `json:"variants"`
}

type EntityResource struct {
	Name         string                                           `json:"name"`
	Type         string                                           `json:"type"`
	Description  string                                           `json:"description"`
	Features     map[string][]metadata.FeatureVariantResource     `json:"features"`
	Labels       map[string][]metadata.LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]metadata.TrainingSetVariantResource `json:"training-sets"`
	Status       string                                           `json:"status"`
	Tags         metadata.Tags                                    `json:"tags"`
	Properties   metadata.Properties                              `json:"properties"`
}

type UserResource struct {
	Name         string                                           `json:"name"`
	Type         string                                           `json:"type"`
	Features     map[string][]metadata.FeatureVariantResource     `json:"features"`
	Labels       map[string][]metadata.LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]metadata.TrainingSetVariantResource `json:"training-sets"`
	Sources      map[string][]metadata.SourceVariantResource      `json:"sources"`
	Status       string                                           `json:"status"`
	Tags         metadata.Tags                                    `json:"tags"`
	Properties   metadata.Properties                              `json:"properties"`
}

type ModelResource struct {
	Name         string                                           `json:"name"`
	Type         string                                           `json:"type"`
	Description  string                                           `json:"description"`
	Features     map[string][]metadata.FeatureVariantResource     `json:"features"`
	Labels       map[string][]metadata.LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]metadata.TrainingSetVariantResource `json:"training-sets"`
	Status       string                                           `json:"status"`
	Tags         metadata.Tags                                    `json:"tags"`
	Properties   metadata.Properties                              `json:"properties"`
}

type ProviderResource struct {
	Name         string                                           `json:"name"`
	Type         string                                           `json:"type"`
	Description  string                                           `json:"description"`
	ProviderType string                                           `json:"provider-type"`
	Software     string                                           `json:"software"`
	Team         string                                           `json:"team"`
	Sources      map[string][]metadata.SourceVariantResource      `json:"sources"`
	Features     map[string][]metadata.FeatureVariantResource     `json:"features"`
	Labels       map[string][]metadata.LabelVariantResource       `json:"labels"`
	TrainingSets map[string][]metadata.TrainingSetVariantResource `json:"training-sets"`
	Status       string                                           `json:"status"`
	Error        string                                           `json:"error"`
	Tags         metadata.Tags                                    `json:"tags"`
	Properties   metadata.Properties                              `json:"properties"`
}

type FetchError struct {
	StatusCode int
	Type       string
}

func (m *FetchError) Error() string {
	return fmt.Sprintf("Error %d: Failed to fetch %s", m.StatusCode, m.Type)
}

func (m *MetadataServer) getTrainingSets(nameVariants []metadata.NameVariant) (map[string][]metadata.TrainingSetVariantResource, error) {
	trainingSetMap := make(map[string][]metadata.TrainingSetVariantResource)
	trainingSetVariants, err := m.client.GetTrainingSetVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range trainingSetVariants {
		if _, has := trainingSetMap[variant.Name()]; !has {
			trainingSetMap[variant.Name()] = []metadata.TrainingSetVariantResource{}
		}
		trainingSetMap[variant.Name()] = append(trainingSetMap[variant.Name()], variant.ToShallowMap())
	}
	return trainingSetMap, nil
}

func (m *MetadataServer) getFeatures(nameVariants []metadata.NameVariant) (map[string][]metadata.FeatureVariantResource, error) {
	featureMap := make(map[string][]metadata.FeatureVariantResource)
	featureVariants, err := m.client.GetFeatureVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range featureVariants {
		if _, has := featureMap[variant.Name()]; !has {
			featureMap[variant.Name()] = []metadata.FeatureVariantResource{}
		}
		featureMap[variant.Name()] = append(featureMap[variant.Name()], variant.ToShallowMap())
	}
	return featureMap, nil
}

func (m *MetadataServer) getLabels(nameVariants []metadata.NameVariant) (map[string][]metadata.LabelVariantResource, error) {
	labelMap := make(map[string][]metadata.LabelVariantResource)
	labelVariants, err := m.client.GetLabelVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range labelVariants {
		if _, has := labelMap[variant.Name()]; !has {
			labelMap[variant.Name()] = []metadata.LabelVariantResource{}
		}
		labelMap[variant.Name()] = append(labelMap[variant.Name()], variant.ToShallowMap())
	}
	return labelMap, nil
}

func (m *MetadataServer) getSources(nameVariants []metadata.NameVariant) (map[string][]metadata.SourceVariantResource, error) {
	sourceMap := make(map[string][]metadata.SourceVariantResource)
	sourceVariants, err := m.client.GetSourceVariants(context.Background(), nameVariants)
	if err != nil {
		return nil, err
	}
	for _, variant := range sourceVariants {
		if _, has := sourceMap[variant.Name()]; !has {
			sourceMap[variant.Name()] = []metadata.SourceVariantResource{}
		}
		sourceMap[variant.Name()] = append(sourceMap[variant.Name()], variant.ToShallowMap())
	}
	return sourceMap, nil
}

func (m *MetadataServer) readFromFeature(feature *metadata.Feature, deepCopy bool) (map[string]metadata.FeatureVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.FeatureVariantResource)
	variants, err := feature.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "feature variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		featResource := variant.ToShallowMap()
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

func (m *MetadataServer) readFromTrainingSet(trainingSet *metadata.TrainingSet, deepCopy bool) (map[string]metadata.TrainingSetVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.TrainingSetVariantResource)
	variants, err := trainingSet.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "training set variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		trainingResource := variant.ToShallowMap()
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

func (m *MetadataServer) readFromSource(source *metadata.Source, deepCopy bool) (map[string]metadata.SourceVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.SourceVariantResource)
	variants, err := source.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "source variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {

		sourceResource := variant.ToShallowMap()
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

func (m *MetadataServer) readFromLabel(label *metadata.Label, deepCopy bool) (map[string]metadata.LabelVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.LabelVariantResource)
	variants, err := label.FetchVariants(m.client, context.Background())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "label variants"}
		m.logger.Errorw(fetchError.Error(), "Internal Error", err)
		return nil, fetchError
	}
	for _, variant := range variants {
		labelResource := variant.ToShallowMap()
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
			Tags:        entity.Tags(),
			Properties:  entity.Properties(),
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
			Name:       user.Name(),
			Type:       "User",
			Status:     user.Status().String(),
			Tags:       user.Tags(),
			Properties: user.Properties(),
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
			Tags:        model.Tags(),
			Properties:  model.Properties(),
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
			Tags:         provider.Tags(),
			Properties:   provider.Properties(),
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
				Tags:        entity.Tags(),
				Properties:  entity.Properties(),
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
				Tags:        model.Tags(),
				Properties:  model.Properties(),
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
				Name:       user.Name(),
				Type:       "User",
				Status:     user.Status().String(),
				Tags:       user.Tags(),
				Properties: user.Properties(),
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
				Tags:         provider.Tags(),
				Properties:   provider.Properties(),
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
	query, ok := c.GetQuery("q")
	if !ok {
		c.JSON(500, "Missing query")
	}

	result, err := SearchClient.RunSearch(query)
	if err != nil {
		m.logger.Errorw("Failed to fetch resources", "error", err)
		c.JSON(500, "Failed to fetch resources")
		return
	}
	c.JSON(200, result)
}

func (m *MetadataServer) GetVersionMap(c *gin.Context) {
	versionMap := map[string]string{
		"version": help.GetEnv("FEATUREFORM_VERSION", ""),
	}
	c.JSON(200, versionMap)
}

type ColumnStat struct {
	Name              string   `json:"name"`
	Type              string   `json:"type"`
	StringCategories  []string `json:"string_categories"`
	NumericCategories [][]int  `json:"numeric_categories"`
	CategoryCounts    []int    `json:"categoryCounts"`
}

type SourceDataResponse struct {
	Columns []string     `json:"columns"`
	Rows    [][]string   `json:"rows"`
	Stats   []ColumnStat `json:"stats"`
}

const MaxPreviewCols = 15

func (m *MetadataServer) GetSourceData(c *gin.Context) {
	name := c.Query("name")
	variant := c.Query("variant")
	var limit int64 = 150
	response := SourceDataResponse{}
	if name == "" || variant == "" {
		fetchError := &FetchError{StatusCode: 400, Type: "GetSourceData - Could not find the name or variant query parameters"}
		m.logger.Errorw(fetchError.Error(), "Metadata error")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	iter, err := m.getSourceDataIterator(name, variant, limit)
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: fmt.Sprintf("GetSourceData - %s", err.Error())}
		m.logger.Errorw(fetchError.Error(), "Metadata error", err)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	for iter.Next() {
		sRow, err := serving.SerializedSourceRow(iter.Values())
		if err != nil {
			fetchError := &FetchError{StatusCode: 500, Type: "GetSourceData"}
			m.logger.Errorw(fetchError.Error(), "Metadata error", err)
			c.JSON(fetchError.StatusCode, fetchError.Error())
			return
		}
		dataRow := []string{}
		for i, rowElement := range sRow.Rows {
			dataRow = append(dataRow, extractElementValue(rowElement))
			if i == MaxPreviewCols {
				dataRow = append(dataRow, "")
				break
			}
		}
		response.Rows = append(response.Rows, dataRow)
	}

	for i, columnName := range iter.Columns() {
		cleanName := strings.ReplaceAll(columnName, "\"", "")
		response.Columns = append(response.Columns, cleanName)
		if i == MaxPreviewCols {
			response.Columns = append(response.Columns, fmt.Sprintf("%d More Columns...", len(iter.Columns())-MaxPreviewCols))
			break
		}
	}

	//intentional
	response.Stats = []ColumnStat{}

	if err := iter.Err(); err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "GetSourceData"}
		m.logger.Errorw(fetchError.Error(), "Metadata error", err)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	c.JSON(200, response)
}

func (m *MetadataServer) GetFeatureFileStats(c *gin.Context) {
	// feature name and variant
	name := c.Query("name")
	variant := c.Query("variant")
	if name == "" || variant == "" {
		fetchError := &FetchError{StatusCode: 400, Type: "GetFeatureFileStats - Could not find the name or variant query parameters"}
		m.logger.Errorw(fetchError.Error(), "Metadata error")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	nameVariant := metadata.NameVariant{Name: name, Variant: variant}
	foundFeatureVariant, err := m.client.GetFeatureVariant(context.Background(), nameVariant)
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Could not get feature variant from metadata"}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	sourceNameVariant := metadata.NameVariant{Name: foundFeatureVariant.Source().Name, Variant: foundFeatureVariant.Source().Variant}
	foundSourceVariant, err := m.client.GetSourceVariant(context.Background(), sourceNameVariant)
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Could not get feature variant from metadata"}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	foundProvider, err := foundSourceVariant.FetchProvider(m.client, context.TODO())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Could not fetch the provider"}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	p, err := provider.Get(pt.Type(foundProvider.Type()), foundProvider.SerializedConfig())
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Could not Get() the provider"}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	sparkOfflineStore, ok := p.(*provider.SparkOfflineStore)
	if !ok {
		fetchError := &FetchError{StatusCode: 405, Type: "Metrics are not currently supported for this provider"}
		m.logger.Errorw(fetchError.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	// Get list of files in the stats directory then return the third part file
	// TODO: move this into provider_schema package
	statsPath := fmt.Sprintf("featureform/Materialization/%s/%s/stats", name, variant)
	statsDirectory, err := sparkOfflineStore.Store.CreateFilePath(statsPath, true)
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Could not create filepath to the stats directory"}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	statsFiles, err := sparkOfflineStore.Store.List(statsDirectory, filestore.JSON)
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Could not list the stats directory"}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	filepath, err := FindFileWithPrefix(statsFiles, "part-00000")
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Could not find the stats file"}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	file, err := sparkOfflineStore.Store.Read(filepath)
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Reading from the file store path failed."}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	response, err := ParseStatFile(file)
	if err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "Parsing the stats file failed."}
		m.logger.Errorw(fetchError.Error(), "error", err.Error())
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	c.JSON(200, response)
}

func FindFileWithPrefix(fileList []filestore.Filepath, prefix string) (filestore.Filepath, error) {
	for _, file := range fileList {
		fileNameSplit := strings.Split(file.Key(), "/")
		fileName := fileNameSplit[len(fileNameSplit)-1] // get the last element which is the file name
		if strings.HasPrefix(fileName, prefix) {
			return file, nil
		}
	}

	return nil, fmt.Errorf("could not find the file prefix %s in the file list", prefix)
}

func ParseStatFile(file []byte) (SourceDataResponse, error) {
	response := SourceDataResponse{}
	var result map[string]interface{}
	err := json.Unmarshal(file, &result)
	if err != nil {
		return response, err
	}
	//todox: change this to a proper struct
	for _, column := range result["columns"].([]interface{}) {
		response.Columns = append(response.Columns, column.(string))
	}
	for _, row := range result["rows"].([]interface{}) {
		rowValues := []string{}
		for _, element := range row.([]interface{}) {
			rowValues = append(rowValues, element.(string))
		}
		response.Rows = append(response.Rows, rowValues)
	}
	for _, col := range result["stats"].([]interface{}) {
		var catCounts []int
		for _, categoryCount := range col.(map[string]interface{})["categoryCounts"].([]interface{}) {
			catCounts = append(catCounts, int(categoryCount.(float64)))
		}

		stringCats := []string{}
		for _, category := range col.(map[string]interface{})["string_categories"].([]interface{}) {
			stringCats = append(stringCats, category.(string))
		}

		numCats := [][]int{}
		for _, category := range col.(map[string]interface{})["numeric_categories"].([]interface{}) {
			innerList := []int{}
			for _, numInterface := range category.([]interface{}) {
				innerList = append(innerList, int(numInterface.(float64)))
			}
			numCats = append(numCats, innerList)
		}

		response.Stats = append(response.Stats, ColumnStat{
			Name:              col.(map[string]interface{})["name"].(string),
			Type:              col.(map[string]interface{})["type"].(string),
			StringCategories:  stringCats,
			NumericCategories: numCats,
			CategoryCounts:    catCounts,
		})
	}
	return response, nil
}

/*
example proto.value args:
double_value:2544
str_value:"C7332112"
*/
func extractElementValue(rowString *proto.Value) string {
	split := strings.Split(rowString.String(), ":")
	result := strings.ReplaceAll(split[1], "\"", "")
	return result
}

func (m *MetadataServer) getSourceDataIterator(name, variant string, limit int64) (provider.GenericTableIterator, error) {
	ctx := context.TODO()
	m.logger.Infow("Getting Source Variant Iterator", "name", name, "variant", variant)
	sv, err := m.client.GetSourceVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, err
	}
	providerEntry, err := sv.FetchProvider(m.client, ctx)
	m.logger.Debugw("Fetched Source Variant Provider", "name", providerEntry.Name(), "type", providerEntry.Type())
	if err != nil {
		return nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, err
	}
	var primary provider.PrimaryTable
	var providerErr error
	if sv.IsTransformation() {
		t, err := store.GetTransformationTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Transformation})
		if err != nil {
			providerErr = err
		} else {
			providerErr = nil
			primary = t.(provider.PrimaryTable)
		}
	} else {
		primary, providerErr = store.GetPrimaryTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Primary})
	}
	if providerErr != nil {
		return nil, providerErr
	}
	return primary.IterateSegment(limit)
}

type VariantResult interface {
	Name() string
	Variant() string
	Tags() metadata.Tags
}

type TagResult struct {
	Name    string   `json:"name"`
	Variant string   `json:"variant"`
	Tags    []string `json:"tags"`
}

func GetTagResult(param VariantResult) TagResult {
	return TagResult{
		Name:    param.Name(),
		Variant: param.Variant(),
		Tags:    param.Tags(),
	}
}

func (m *MetadataServer) GetTagError(code int, err error, c *gin.Context, resourceType string) *FetchError {
	fetchError := &FetchError{StatusCode: code, Type: resourceType}
	m.logger.Errorw(fetchError.Error(), "Metadata error", err)
	return fetchError
}

func (m *MetadataServer) SetFoundVariantJSON(foundVariant VariantResult, err error, c *gin.Context, resourceType string) {
	if err != nil {
		fetchError := m.GetTagError(500, err, c, resourceType)
		c.JSON(fetchError.StatusCode, fetchError.Error())
	}
	c.JSON(http.StatusOK, GetTagResult(foundVariant))
}

type TagGetBody struct {
	Variant string `json:"variant"`
}

func (m *MetadataServer) GetTags(c *gin.Context) {
	name := c.Param("resource")
	resourceType := c.Param("type")
	var requestBody TagGetBody
	if err := c.BindJSON(&requestBody); err != nil {
		fetchError := m.GetTagError(500, err, c, "GetTags - Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	nameVariant := metadata.NameVariant{Name: name, Variant: requestBody.Variant}
	switch resourceType {
	case "features":
		foundVariant, err := m.client.GetFeatureVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "labels":
		foundVariant, err := m.client.GetLabelVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "training-sets":
		foundVariant, err := m.client.GetTrainingSetVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "sources":
		foundVariant, err := m.client.GetSourceVariant(context.Background(), nameVariant)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "entities":
		foundVariant, err := m.client.GetEntity(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "users":
		foundVariant, err := m.client.GetUser(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "models":
		foundVariant, err := m.client.GetModel(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	case "providers":
		foundVariant, err := m.client.GetProvider(context.Background(), name)
		m.SetFoundVariantJSON(foundVariant, err, c, resourceType)
	}
}

type TagPostBody struct {
	Tags    []string `json:"tags"`
	Variant string   `json:"variant"`
}

func getResourceType(resourceTypeString string) metadata.ResourceType {
	var resourceType metadata.ResourceType
	switch resourceTypeString {
	case "features":
		resourceType = metadata.FEATURE_VARIANT
	case "labels":
		resourceType = metadata.LABEL_VARIANT
	case "training-sets":
		resourceType = metadata.TRAINING_SET_VARIANT
	case "sources":
		resourceType = metadata.SOURCE_VARIANT
	case "entities":
		resourceType = metadata.ENTITY
	case "users":
		resourceType = metadata.USER
	case "models":
		resourceType = metadata.MODEL
	case "providers":
		resourceType = metadata.PROVIDER
	}
	return resourceType
}

func (m *MetadataServer) PostTags(c *gin.Context) {
	var requestBody TagPostBody
	resourceTypeParam := c.Param("type")
	if err := c.BindJSON(&requestBody); err != nil {
		fetchError := m.GetTagError(500, err, c, "PostTags - Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	resourceType := getResourceType(resourceTypeParam)
	name := c.Param("resource")
	variant := requestBody.Variant

	objID := metadata.ResourceID{
		Name:    name,
		Variant: variant,
		Type:    resourceType,
	}
	foundResource, err := m.lookup.Lookup(c, objID)

	if err != nil {
		fetchError := m.GetTagError(400, err, c, "PostTags - Error finding the resource with resourceID")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	replaceTags(resourceTypeParam, foundResource, &pb.Tags{Tag: requestBody.Tags})

	m.lookup.Set(objID, foundResource)

	updatedResource := search.ResourceDoc{
		Name:    name,
		Variant: variant,
		Type:    resourceType.String(),
		Tags:    requestBody.Tags,
	}
	// Update search index for Meilisearch
	SearchClient.Upsert(updatedResource)

	c.JSON(http.StatusOK, TagResult{
		Name:    name,
		Variant: variant,
		Tags:    requestBody.Tags,
	})
}

func replaceTags(resourceTypeParam string, currentResource metadata.Resource, newTagList *pb.Tags) error {
	deserialized := currentResource.Proto()
	switch resourceTypeParam {
	case "features":
		variantUpdate, ok := deserialized.(*pb.FeatureVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "labels":
		variantUpdate, ok := deserialized.(*pb.LabelVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "training-sets":
		variantUpdate, ok := deserialized.(*pb.TrainingSetVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "sources":
		variantUpdate, ok := deserialized.(*pb.SourceVariant)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "entities":
		variantUpdate, ok := deserialized.(*pb.Entity)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "users":
		variantUpdate, ok := deserialized.(*pb.User)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "models":
		variantUpdate, ok := deserialized.(*pb.Model)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	case "providers":
		variantUpdate, ok := deserialized.(*pb.Provider)
		if !ok {
			return errors.New("replaceTags - Failed to deserialize variant")
		}
		variantUpdate.Tags.Reset()
		variantUpdate.Tags = newTagList
	}
	return nil
}

func (m *MetadataServer) Start(port string) {
	router := gin.Default()
	router.Use(cors.Default())
	router.GET("/data/:type", m.GetMetadataList)
	router.GET("/data/:type/:resource", m.GetMetadata)
	router.GET("/data/search", m.GetSearch)
	router.GET("/data/version", m.GetVersionMap)
	router.GET("/data/sourcedata", m.GetSourceData)
	router.GET("/data/filestatdata", m.GetFeatureFileStats)
	router.POST("/data/:type/:resource/gettags", m.GetTags)
	router.POST("/data/:type/:resource/tags", m.PostTags)
	router.Run(port)
}

func main() {
	logger := zap.NewExample().Sugar()
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	searchHost := help.GetEnv("MEILISEARCH_HOST", "localhost")
	searchPort := help.GetEnv("MEILISEARCH_PORT", "7700")
	searchEndpoint := fmt.Sprintf("http://%s:%s", searchHost, searchPort)
	searchApiKey := help.GetEnv("MEILISEARCH_APIKEY", "xyz")
	logger.Infof("Connecting to typesense at: %s\n", searchEndpoint)
	sc, err := search.NewMeilisearch(&search.MeilisearchParams{
		Host:   searchHost,
		Port:   searchPort,
		ApiKey: searchApiKey,
	})
	if err != nil {
		logger.Panicw("Failed to create new meil search", err)
	}

	SearchClient = sc
	metadataAddress := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	logger.Infof("Looking for metadata at: %s\n", metadataAddress)
	client, err := metadata.NewClient(metadataAddress, logging.WrapZapLogger(logger))
	if err != nil {
		logger.Panicw("Failed to connect", "error", err)
	}

	etcdHost := help.GetEnv("ETCD_HOST", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	storageProvider := metadata.EtcdStorageProvider{
		Config: metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{Host: etcdHost, Port: etcdPort},
			},
		},
	}

	metadataServer, err := NewMetadataServer(logger, client, &storageProvider)
	if err != nil {
		logger.Panicw("Failed to create server", "error", err)
	}
	metadataHTTPPort := help.GetEnv("METADATA_HTTP_PORT", "3001")
	metadataServingPort := fmt.Sprintf(":%s", metadataHTTPPort)
	logger.Infof("Serving HTTP Metadata on port: %s\n", metadataServingPort)
	metadataServer.Start(metadataServingPort)
}
