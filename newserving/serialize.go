// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package newserving

import (
	"fmt"

	pb "github.com/featureform/proto"
)

type feature struct {
	serialized *pb.Value
}

func newFeature(val interface{}) (*feature, error) {
	serial, err := wrapValue(val)
	if err != nil {
		return nil, fmt.Errorf("new feature: %w", err)
	}
	return &feature{serial}, nil
}

func (f *feature) Serialized() *pb.Value {
	return f.serialized
}

type row struct {
	serialized *pb.TrainingDataRow
}

func emptyRow() *row {
	return &row{
		serialized: &pb.TrainingDataRow{},
	}
}

func serializedRow(features []interface{}, label interface{}) (*pb.TrainingDataRow, error) {
	r, err := newRow(features, label)
	if err != nil {
		return nil, err
	}
	return r.Serialized(), nil
}

func newRow(features []interface{}, label interface{}) (*row, error) {
	r := emptyRow()
	for _, f := range features {
		if err := r.AddFeature(f); err != nil {
			return nil, err
		}
	}
	if err := r.SetLabel(label); err != nil {
		return nil, err
	}
	return r, nil
}

func (row *row) Serialized() *pb.TrainingDataRow {
	return row.serialized
}

func (row *row) SetLabel(label interface{}) error {
	value, err := wrapValue(label)
	if err != nil {
		return fmt.Errorf("set label: %w", err)
	}
	row.serialized.Label = value
	return nil
}

func (row *row) AddFeature(feature interface{}) error {
	value, err := wrapValue(feature)
	if err != nil {
		return fmt.Errorf("add feature: %w", err)
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
	case float64:
		proto = wrapDouble(typed)
	case int:
		proto = wrapInt(typed)
	case int32:
		proto = wrapInt32(typed)
	case int64:
		proto = wrapInt64(typed)
	case bool:
		proto = wrapBool(typed)
	case *pb.Value:
		proto = typed
	case nil:
		proto = wrapNil(typed)
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

func wrapDouble(val float64) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_DoubleValue{val},
	}
}

func wrapStr(val string) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_StrValue{val},
	}
}

func wrapInt(val int) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_IntValue{int32(val)},
	}
}

func wrapInt32(val int32) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_Int32Value{val},
	}
}

func wrapInt64(val int64) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_Int64Value{val},
	}
}

func wrapBool(val bool) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_BoolValue{val},
	}
}

func wrapNil(val interface{}) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_StrValue{""},
	}
}
