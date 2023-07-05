// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	help "github.com/featureform/helpers"
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

var searchClient search.Searcher

type StorageProvider interface {
	GetResourceLookup() (metadata.ResourceLookup, error)
}

type LocalStorageProvider struct {
}

func (sp LocalStorageProvider) GetResourceLookup() (metadata.ResourceLookup, error) {
	lookup := make(metadata.LocalResourceLookup)
	return lookup, nil
}

type EtcdStorageProvider struct {
	Config metadata.EtcdConfig
}

func (sp EtcdStorageProvider) GetResourceLookup() (metadata.ResourceLookup, error) {

	client, err := sp.Config.InitClient()
	if err != nil {
		return nil, fmt.Errorf("could not init etcd client: %v", err)
	}
	lookup := metadata.EtcdResourceLookup{
		Connection: metadata.EtcdStorage{
			Client: client,
		},
	}
	return lookup, nil
}

type MetadataServer struct {
	lookup          metadata.ResourceLookup
	client          *metadata.Client
	logger          *zap.SugaredLogger
	StorageProvider StorageProvider
}

func NewMetadataServer(logger *zap.SugaredLogger, client *metadata.Client) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	etcdHost := help.GetEnv("ETCD_HOST", "featureform-etcd")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	storageProvider := metadata.EtcdStorageProvider{
		Config: metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{Host: etcdHost, Port: etcdPort},
			},
		},
	}
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

func columnsToMap(columns metadata.ResourceVariantColumns) map[string]string {
	columnNameValues := reflect.ValueOf(columns)
	featureColumns := make(map[string]string)
	for i := 0; i < columnNameValues.NumField(); i++ {
		featureColumns[columnNameValues.Type().Field(i).Name] = fmt.Sprintf("%v", columnNameValues.Field(i).Interface())
	}
	return featureColumns
}

func featureShallowMap(variant *metadata.FeatureVariant) metadata.FeatureVariantResource {
	fv := metadata.FeatureVariantResource{}
	switch variant.Mode() {
	case metadata.PRECOMPUTED:
		fv = metadata.FeatureVariantResource{
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
			Tags:        variant.Tags(),
			Properties:  variant.Properties(),
			Mode:        variant.Mode().String(),
			IsOnDemand:  variant.IsOnDemand(),
		}
	case metadata.CLIENT_COMPUTED:
		location := make(map[string]string)
		if pyFunc, ok := variant.LocationFunction().(metadata.PythonFunction); ok {
			location["query"] = string(pyFunc.Query)
		}
		fv = metadata.FeatureVariantResource{
			Created:     variant.Created(),
			Description: variant.Description(),
			Name:        variant.Name(),
			Variant:     variant.Variant(),
			Owner:       variant.Owner(),
			Location:    location,
			Status:      variant.Status().String(),
			Error:       variant.Error(),
			Tags:        variant.Tags(),
			Properties:  variant.Properties(),
			Mode:        variant.Mode().String(),
			IsOnDemand:  variant.IsOnDemand(),
		}
	default:
		fmt.Printf("Unknown computation mode %v\n", variant.Mode())
	}
	return fv
}

func labelShallowMap(variant *metadata.LabelVariant) metadata.LabelVariantResource {
	return metadata.LabelVariantResource{
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
		Tags:        variant.Tags(),
		Properties:  variant.Properties(),
	}
}

func trainingSetShallowMap(variant *metadata.TrainingSetVariant) metadata.TrainingSetVariantResource {
	return metadata.TrainingSetVariantResource{
		Created:     variant.Created(),
		Description: variant.Description(),
		Name:        variant.Name(),
		Variant:     variant.Variant(),
		Owner:       variant.Owner(),
		Provider:    variant.Provider(),
		Label:       variant.Label(),
		Status:      variant.Status().String(),
		Error:       variant.Error(),
		Tags:        variant.Tags(),
		Properties:  variant.Properties(),
	}
}

func sourceShallowMap(variant *metadata.SourceVariant) metadata.SourceVariantResource {
	return metadata.SourceVariantResource{
		Name:           variant.Name(),
		Variant:        variant.Variant(),
		Definition:     getSourceString(variant),
		Owner:          variant.Owner(),
		Description:    variant.Description(),
		Provider:       variant.Provider(),
		Created:        variant.Created(),
		Status:         variant.Status().String(),
		LastUpdated:    variant.LastUpdated(),
		Schedule:       variant.Schedule(),
		Tags:           variant.Tags(),
		SourceType:     getSourceType(variant),
		Properties:     variant.Properties(),
		Error:          variant.Error(),
		Specifications: getSourceArgs(variant),
	}
}

func getSourceString(variant *metadata.SourceVariant) string {
	if variant.IsSQLTransformation() {
		return variant.SQLTransformationQuery()
	} else {
		return variant.PrimaryDataSQLTableName()
	}
}

func getSourceType(variant *metadata.SourceVariant) string {
	if variant.IsSQLTransformation() {
		return "SQL Transformation"
	} else if variant.IsDFTransformation() {
		return "Dataframe Transformation"
	} else {
		return "Primary Table"
	}
}

func getSourceArgs(variant *metadata.SourceVariant) map[string]string {
	if variant.HasKubernetesArgs() {
		return variant.TransformationArgs().Format()
	}
	return map[string]string{}
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
		trainingSetMap[variant.Name()] = append(trainingSetMap[variant.Name()], trainingSetShallowMap(variant))
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
		featureMap[variant.Name()] = append(featureMap[variant.Name()], featureShallowMap(variant))
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
		labelMap[variant.Name()] = append(labelMap[variant.Name()], labelShallowMap(variant))
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
		sourceMap[variant.Name()] = append(sourceMap[variant.Name()], sourceShallowMap(variant))
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

func (m *MetadataServer) readFromTrainingSet(trainingSet *metadata.TrainingSet, deepCopy bool) (map[string]metadata.TrainingSetVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.TrainingSetVariantResource)
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

func (m *MetadataServer) readFromSource(source *metadata.Source, deepCopy bool) (map[string]metadata.SourceVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.SourceVariantResource)
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

func (m *MetadataServer) readFromLabel(label *metadata.Label, deepCopy bool) (map[string]metadata.LabelVariantResource, *FetchError) {
	variantMap := make(map[string]metadata.LabelVariantResource)
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

	result, err := searchClient.RunSearch(query)
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

type SourceDataResponse struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
}

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
		fetchError := &FetchError{StatusCode: 500, Type: "GetSourceData - getSourceDataIterator() threw an exception"}
		m.logger.Errorw(fetchError.Error(), "Metadata error", err)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	for _, columnName := range iter.Columns() {
		response.Columns = append(response.Columns, strings.ReplaceAll(columnName, "\"", ""))
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
		for _, rowElement := range sRow.Rows {
			dataRow = append(dataRow, extractRowValue(rowElement))
		}
		response.Rows = append(response.Rows, dataRow)
	}

	if err := iter.Err(); err != nil {
		fetchError := &FetchError{StatusCode: 500, Type: "GetSourceData"}
		m.logger.Errorw(fetchError.Error(), "Metadata error", err)
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}
	c.JSON(200, response)
}

/*
example proto.value args:
double_value:2544
str_value:"C7332112"
*/
func extractRowValue(rowString *proto.Value) string {
	split := strings.Split(rowString.String(), ":")
	result := strings.ReplaceAll(split[1], "\"", "")
	return result
}

func (m *MetadataServer) getSourceDataIterator(name, variant string, limit int64) (provider.GenericTableIterator, error) {
	ctx := context.TODO()
	m.logger.Infow("Getting Source Variant Iterator", "name", name, "variant", variant)
	sv, err := m.client.GetSourceVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get source variant")
	}
	providerEntry, err := sv.FetchProvider(m.client, ctx)
	m.logger.Debugw("Fetched Source Variant Provider", "name", providerEntry.Name(), "type", providerEntry.Type())
	if err != nil {
		return nil, errors.Wrap(err, "could not get fetch provider")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, errors.Wrap(err, "could not open as offline store")
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
		return nil, errors.Wrap(err, "could not get primary table")
	}
	return primary.IterateSegment(limit)
}

type VariantDuck interface {
	Name() string
	Variant() string
	Tags() metadata.Tags
}

type TagResult struct {
	Name    string   `json:"name"`
	Variant string   `json:"variant"`
	Tags    []string `json:"tags"`
}

func GetTagResult(param VariantDuck) TagResult {
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

func (m *MetadataServer) SetFoundVariantJSON(foundVariant VariantDuck, err error, c *gin.Context, resourceType string) {
	if err != nil {
		fetchError := m.GetTagError(500, err, c, resourceType)
		c.JSON(fetchError.StatusCode, fetchError.Error())
	}
	c.JSON(http.StatusOK, GetTagResult(foundVariant))
}

func (m *MetadataServer) GetTags(c *gin.Context) {
	name := c.Param("resource")
	resourceType := c.Param("type")
	nameVariant := metadata.NameVariant{Name: name, Variant: "default"}
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

type TagRequestBody struct {
	Tags []string `json:"tags"`
}

func (m *MetadataServer) PostTags(c *gin.Context) {
	var requestBody TagRequestBody
	if err := c.BindJSON(&requestBody); err != nil {
		fetchError := m.GetTagError(500, err, c, "PostTags - Error binding the request body")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	name := c.Param("resource")
	variant := "default"
	resourceType := metadata.SOURCE_VARIANT

	objID := metadata.ResourceID{
		Name:    name,
		Variant: variant,
		Type:    resourceType,
	}
	foundResource, err := m.lookup.Lookup(objID)

	if err != nil {
		fetchError := m.GetTagError(400, err, c, "PostTags - Error finding the resource with resourceID")
		c.JSON(fetchError.StatusCode, fetchError.Error())
		return
	}

	replaceTags(foundResource, &pb.Tags{Tag: requestBody.Tags})

	m.lookup.Set(objID, foundResource)

	c.JSON(http.StatusOK, TagResult{
		Name:    name,
		Variant: variant,
		Tags:    requestBody.Tags,
	})
}

func replaceTags(currentResource metadata.Resource, newTagList *pb.Tags) error {
	deserialized := currentResource.Proto()
	variantUpdate, ok := deserialized.(*pb.SourceVariant) //todox: create a switch for the types
	if !ok {
		return errors.New("replaceTags - Failed to deserialize variant")
	}
	variantUpdate.Tags.Reset()
	variantUpdate.Tags = newTagList
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
	router.GET("/data/:type/:resource/tags", m.GetTags)
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

	searchClient = sc
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
