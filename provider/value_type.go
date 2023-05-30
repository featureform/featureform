// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

type ValueType interface {
	Scalar() ScalarType
	IsVector() bool
}

type VectorType struct {
	ScalarType  ScalarType
	Dimension   uint32
	IsEmbedding bool
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
