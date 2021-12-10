package main

import (
	"fmt"

	pb "github.com/featureform/serving/proto"
)

type DatasetProvider interface {
	GetDatasetReader(key map[string]string) (DatasetReader, error)
}

type DatasetReader interface {
	Scan() bool
	Row() *TrainingDataRow
	Err() error
}

type TrainingDataRow struct {
	serialized *pb.TrainingDataRow
}

func NewTrainingDataRow() *TrainingDataRow {
	return &TrainingDataRow{
		serialized: &pb.TrainingDataRow{},
	}
}

func (row *TrainingDataRow) Serialized() *pb.TrainingDataRow {
	return row.serialized
}

func (row *TrainingDataRow) SetLabel(label interface{}) error {
	value, err := wrapValue(label)
	if err != nil {
		return err
	}
	row.serialized.Label = value
	return nil
}

func (row *TrainingDataRow) AddFeature(feature interface{}) error {
	value, err := wrapValue(feature)
	if err != nil {
		return err
	}
	row.serialized.Features = append(row.serialized.Features, value)
	return nil
}

type InvalidValue struct {
	Value interface{}
}

func (err InvalidValue) Error() string {
	return fmt.Sprintf("Invalid Value Type: %T", err.Value)
}

func wrapValue(value interface{}) (proto *pb.Value, err error) {
	switch typed := value.(type) {
	case string:
		proto = wrapStr(typed)
	case float32:
		proto = wrapFloat(typed)
	case int32:
		proto = wrapInt(typed)
	case *pb.Value:
		proto = typed
	default:
		err = InvalidValue{value}
	}
	return
}

func wrapFloat(val float32) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_FloatValue{val},
	}
}

func wrapStr(val string) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_StrValue{val},
	}
}

func wrapInt(val int32) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_IntValue{val},
	}
}

type Type int

const (
	String Type = iota
	Float
	Int
)
