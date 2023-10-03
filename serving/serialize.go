// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package serving

import (
	"fmt"
	"time"

	"github.com/featureform/metadata"
	pb "github.com/featureform/proto"
)

type value struct {
	serialized *pb.Value
}

func newValue(val interface{}) (*value, error) {
	serial, err := wrapValue(val)
	if err != nil {
		return nil, fmt.Errorf("new value: %w", err)
	}
	return &value{serial}, nil
}

func (v *value) Serialized() *pb.Value {
	return v.serialized
}

type row struct {
	serialized *pb.TrainingDataRow
}

type sourceRow struct {
	serialized *pb.SourceDataRow
}

func emptyRow() *row {
	return &row{
		serialized: &pb.TrainingDataRow{},
	}
}

func emptySourceRow() *sourceRow {
	return &sourceRow{
		serialized: &pb.SourceDataRow{},
	}
}

func serializedRow(features []interface{}, label interface{}) (*pb.TrainingDataRow, error) {
	r, err := newRow(features, label)
	if err != nil {
		return nil, err
	}
	return r.Serialized(), nil
}

func SerializedSourceRow(row []interface{}) (*pb.SourceDataRow, error) {
	r, err := newSourceRow(row)
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

func newSourceRow(rows []interface{}) (*sourceRow, error) {
	r := emptySourceRow()
	for _, row := range rows {
		if err := r.AddValue(row); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (row *sourceRow) Serialized() *pb.SourceDataRow {
	return row.serialized
}

func (r *sourceRow) AddValue(row interface{}) error {
	value, err := wrapValue(row)
	if err != nil {
		return fmt.Errorf("add row: %w", err)
	}
	r.serialized.Rows = append(r.serialized.Rows, value)
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
	/*
		We should eventually add support for native time types; however, at
		the moment, supporting RFC3339 strings will suffice.
	*/
	case time.Time:
		proto = wrapStr(typed.Format(time.RFC3339))
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
	case metadata.PythonFunction:
		proto = wrapBytes(typed.Query)
	case *pb.Value:
		proto = typed
	case nil:
		proto = wrapNil(typed)
	case []float32:
		proto = wrapVec32(typed)
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

func wrapBytes(val []byte) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_OnDemandFunction{val},
	}
}

func wrapVec32(val []float32) *pb.Value {
	return &pb.Value{
		Value: &pb.Value_Vector32Value{
			Vector32Value: &pb.Vector32{
				Value: val,
			},
		},
	}
}
