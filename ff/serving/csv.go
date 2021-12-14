package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	pb "github.com/featureform/serving/proto"
	"go.uber.org/zap"
)

type LocalCSVProvider struct {
	Logger *zap.SugaredLogger
}

func (provider *LocalCSVProvider) GetDatasetReader(key map[string]string) (DatasetReader, error) {
	logger := provider.Logger.With("Key", key)
	logger.Debug("Finding Dataset Reader")
	schemaJson, has := key["schema"]
	if !has {
		errMsg := "Schema not found in key"
		logger.Error(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	var schema CSVSchema
	if err := json.Unmarshal([]byte(schemaJson), &schema); err != nil {
		logger.Errorw("Invalid JSON schema", "Error", err)
		return nil, fmt.Errorf("Invalid Schema JSON %s: %s", schemaJson, err)
	}
	path, has := key["path"]
	if !has {
		logger.Error("Path not found in schema")
		return nil, fmt.Errorf("Path not found in schema")
	}
	f, err := os.Open(path)
	if err != nil {
		logger.Errorw("Failed to open file", "Error", err)
		return nil, err
	}
	return NewCSVDataset(f, schema)
}

func (provider *LocalCSVProvider) ToKey(path string, schema CSVSchema) map[string]string {
	key := make(map[string]string)
	schemaJson, err := json.Marshal(schema)
	if err != nil {
		panic(err)
	}
	key["schema"] = string(schemaJson)
	key["path"] = path
	logger.Debugw("Generated key from schema", "Key", key, "Schema", schema)
	return key
}

type CSVDataset struct {
	reader     *csv.Reader
	mapping    *csvMapping
	scannedRow *TrainingDataRow
	curErr     error
}

type CSVSchema struct {
	HasHeader bool
	Header    []string
	Features  []string
	Label     string
	Types     map[string]Type
}

func (schema CSVSchema) toMapping() (*csvMapping, error) {
	reverseIdx := make(map[string]int)
	for i, header := range schema.Header {
		reverseIdx[header] = i
	}
	labelIdx, has := reverseIdx[schema.Label]
	if !has {
		return nil, fmt.Errorf("Label not in header: %v", schema)
	}
	labelType, has := schema.Types[schema.Label]
	if !has {
		return nil, fmt.Errorf("Label type not found: %v", schema)
	}
	labelCastFn, err := getCsvCastFn(labelType)
	if err != nil {
		return nil, err
	}

	featureIdxs := make([]int, len(schema.Features))
	featureCastFns := make([]csvCastFn, len(schema.Features))
	for i, feature := range schema.Features {
		idx, has := reverseIdx[feature]
		if !has {
			return nil, fmt.Errorf("Not all fields are in header: %v", schema)
		}
		featureType, has := schema.Types[feature]
		if !has {
			return nil, fmt.Errorf("Feature %s type not found: %v", feature, schema)
		}
		castFn, err := getCsvCastFn(featureType)
		if err != nil {
			return nil, err
		}
		featureIdxs[i] = idx
		featureCastFns[i] = castFn
	}

	return &csvMapping{
		LabelIdx:       labelIdx,
		FeatureIdxs:    featureIdxs,
		LabelCastFn:    labelCastFn,
		FeatureCastFns: featureCastFns,
	}, nil
}

type csvCastFn func(string) (*pb.Value, error)

func getCsvCastFn(t Type) (csvCastFn, error) {
	switch t {
	case String:
		return func(str string) (*pb.Value, error) {
			return wrapStr(str), nil
		}, nil
	case Float:
		return func(str string) (*pb.Value, error) {
			flt, err := strconv.ParseFloat(str, 32)
			if err != nil {
				return nil, err
			}
			return wrapFloat(float32(flt)), nil
		}, nil
	case Int:
		return func(str string) (*pb.Value, error) {
			num, err := strconv.ParseInt(str, 10, 32)
			if err != nil {
				return nil, err
			}
			return wrapInt(int32(num)), nil
		}, nil
	}
	return nil, fmt.Errorf("Unknown type")
}

type csvMapping struct {
	LabelIdx       int
	FeatureIdxs    []int
	FeatureCastFns []csvCastFn
	LabelCastFn    csvCastFn
}

func NewCSVDataset(f io.Reader, schema CSVSchema) (*CSVDataset, error) {
	reader := csv.NewReader(f)

	if schema.HasHeader {
		header, err := reader.Read()
		if err != nil {
			return nil, err
		}
		schema.Header = header
	}

	mapping, err := schema.toMapping()
	if err != nil {
		return nil, err
	}

	return &CSVDataset{
		reader:  reader,
		mapping: mapping,
	}, nil
}

func (dataset *CSVDataset) Scan() bool {
	row, err := dataset.reader.Read()
	if err != nil {
		if err != io.EOF {
			dataset.curErr = err
		}
		return false
	}
	trainRow := NewTrainingDataRow()
	mapping := dataset.mapping
	for i, csvIdx := range mapping.FeatureIdxs {
		castFn := mapping.FeatureCastFns[i]
		val := row[csvIdx]
		casted, err := castFn(val)
		if err != nil {
			dataset.curErr = err
			return false
		}
		addErr := trainRow.AddFeature(casted)
		if addErr != nil {
			dataset.curErr = addErr
			return false
		}
	}
	label, err := mapping.LabelCastFn(row[mapping.LabelIdx])
	if err != nil {
		dataset.curErr = err
		return false
	}
	setErr := trainRow.SetLabel(label)
	if setErr != nil {
		dataset.curErr = setErr
		return false
	}
	dataset.scannedRow = trainRow
	return true
}

func (dataset *CSVDataset) Row() *TrainingDataRow {
	return dataset.scannedRow
}

func (dataset *CSVDataset) Err() error {
	return dataset.curErr
}
