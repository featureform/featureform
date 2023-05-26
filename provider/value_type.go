// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import "fmt"

type ValueType interface {
	Scalar() ScalarType
	IsVector() bool
}

type VectorType struct {
	ScalarType ScalarType
	Dimension  int
	// InitialCapacity int
}

func (t VectorType) Scalar() ScalarType {
	return t.ScalarType
}

func (t VectorType) IsVector() bool {
	return true
}

type ScalarType string

func (t ScalarType) Scalar() ScalarType {
	return t
}

func (t ScalarType) IsVector() bool {
	return false
}

const (
	NilType   ScalarType = ""
	Int       ScalarType = "int"
	Int32     ScalarType = "int32"
	Int64     ScalarType = "int64"
	Float32   ScalarType = "float32"
	Float64   ScalarType = "float64"
	String    ScalarType = "string"
	Bool      ScalarType = "bool"
	Timestamp ScalarType = "time.Time"
	Datetime  ScalarType = "datetime"
)

func NewValueType(scalar string, isVector bool, dim int) (ValueType, error) {
	var scalarType ScalarType
	switch scalar {
	case "int":
		scalarType = Int
	case "int32":
		scalarType = Int32
	case "int64":
		scalarType = Int64
	case "float32":
		scalarType = Float32
	case "float64":
		scalarType = Float64
	case "string":
		scalarType = String
	case "bool":
		scalarType = Bool
	case "time.Time":
		scalarType = Timestamp
	case "datetime":
		scalarType = Datetime
	case "":
		scalarType = NilType
	default:
		return nil, fmt.Errorf("unknown scalar value: %s", scalar)
	}
	var valueType ValueType
	if isVector {
		valueType = VectorType{ScalarType: scalarType, Dimension: dim}
	} else {
		valueType = scalarType
	}
	return valueType, nil
}
