package dataset

import (
	"fmt"

	pb "github.com/featureform/serving/proto"
)

type OfflineProvider interface {
	GetDatasetReader(key map[string]string) (Reader, error)
}

type OnlineProvider interface {
	GetFeatureLookup(key map[string]string) (Lookup, error)
}

type Reader interface {
	Scan() bool
	Row() *Row
	Err() error
}

type Lookup interface {
	Get(entity string) (*Feature, error)
}

type Feature struct {
	serialized *pb.Value
}

func NewFeature(val interface{}) (*Feature, error) {
	serial, err := WrapValue(val)
	if err != nil {
		return nil, err
	}
	return &Feature{serial}, nil
}

func (f *Feature) Serialized() *pb.Value {
	return f.serialized
}

type Row struct {
	serialized *pb.TrainingDataRow
}

func NewRow() *Row {
	return &Row{
		serialized: &pb.TrainingDataRow{},
	}
}

func (row *Row) Serialized() *pb.TrainingDataRow {
	return row.serialized
}

func (row *Row) SetLabel(label interface{}) error {
	value, err := WrapValue(label)
	if err != nil {
		return err
	}
	row.serialized.Label = value
	return nil
}

func (row *Row) AddFeature(feature interface{}) error {
	value, err := WrapValue(feature)
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

func WrapValue(value interface{}) (proto *pb.Value, err error) {
	switch typed := value.(type) {
	case string:
		proto = WrapStr(typed)
	case float32:
		proto = WrapFloat(typed)
	case float64:
		proto = WrapDouble(typed)
	case int32:
		proto = WrapInt(typed)
	case int64:
		proto = WrapInt64(typed)
	case *pb.Value:
		proto = typed
	default:
		err = InvalidValue{value}
	}
	return
}

func WrapFloat(val float32) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_FloatValue{val},
	}
}

func WrapDouble(val float64) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_DoubleValue{val},
	}
}

func WrapStr(val string) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_StrValue{val},
	}
}

func WrapInt(val int32) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_IntValue{val},
	}
}

func WrapInt64(val int64) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_Int64Value{val},
	}
}

type Type int

const (
	String Type = iota
	Float
	Int
)
