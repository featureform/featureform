package types

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
	db "github.com/jackc/pgx/v4"

	"github.com/featureform/fferr"
	pb "github.com/featureform/metadata/proto"
)

func init() {
	initProtoToScalar()
}

const (
	Int       ScalarType = "int"
	Int8      ScalarType = "int8"
	Int16     ScalarType = "int16"
	Int32     ScalarType = "int32"
	Int64     ScalarType = "int64"
	UInt8     ScalarType = "uint8"
	UInt16    ScalarType = "uint16"
	UInt32    ScalarType = "uint32"
	UInt64    ScalarType = "uint64"
	Float32   ScalarType = "float32"
	Float64   ScalarType = "float64"
	String    ScalarType = "string"
	Bool      ScalarType = "bool"
	Timestamp ScalarType = "time.Time"
	Datetime  ScalarType = "datetime"
	Unknown   ScalarType = "Unknown"
)

var ScalarTypes = map[ScalarType]bool{
	Int:       true,
	Int8:      true,
	Int16:     true,
	Int32:     true,
	Int64:     true,
	UInt8:     true,
	UInt16:    true,
	UInt32:    true,
	UInt64:    true,
	Float32:   true,
	Float64:   true,
	String:    true,
	Bool:      true,
	Timestamp: true,
	Datetime:  true,
}

var scalarToProto = map[ScalarType]pb.ScalarType{
	//NilType: pb.ScalarType_NULL,
	Int:     pb.ScalarType_INT,
	Int32:   pb.ScalarType_INT32,
	Int64:   pb.ScalarType_INT64,
	Float32: pb.ScalarType_FLOAT32,
	Float64: pb.ScalarType_FLOAT64,
	String:  pb.ScalarType_STRING,
	Bool:    pb.ScalarType_BOOL,
}

// Created in init() as the inverse of scalarToProto
var protoToScalar map[pb.ScalarType]ScalarType

func initProtoToScalar() {
	protoToScalar = make(map[pb.ScalarType]ScalarType, len(scalarToProto))
	for scalar, proto := range scalarToProto {
		protoToScalar[proto] = scalar
	}
}

type ValueType interface {
	Scalar() ScalarType
	IsVector() bool
	Type() reflect.Type
	String() string
	ToProto() *pb.ValueType
}

type VectorType struct {
	ScalarType  ScalarType
	Dimension   int32
	IsEmbedding bool
}

func ValueTypeFromProto(protoVal *pb.ValueType) (ValueType, error) {
	switch casted := protoVal.GetType().(type) {
	case *pb.ValueType_Scalar:
		scalar, has := protoToScalar[casted.Scalar]
		if has {
			return scalar, nil
		}
	case *pb.ValueType_Vector:
		protoVec := casted.Vector
		scalar, has := protoToScalar[protoVec.Scalar]
		if has {
			return VectorType{
				ScalarType:  scalar,
				Dimension:   protoVec.Dimension,
				IsEmbedding: protoVec.IsEmbedding,
			}, nil
		}
	}
	protoStr := proto.MarshalTextString(protoVal)
	return nil, fferr.NewInternalErrorf("Unable to parse value type proto %T %s", protoVal.GetType(), protoStr)
}

// jsonValueType provides a generic JSON representation of any ValueType.
type jsonValueType struct {
	ScalarType  ScalarType
	Dimension   int32
	IsEmbedding bool
	IsVector    bool
}

func (wrapper *jsonValueType) FromValueType(t ValueType) {
	switch typed := t.(type) {
	case ScalarType:
		*wrapper = jsonValueType{
			ScalarType: typed,
			IsVector:   false,
		}
	case VectorType:
		*wrapper = jsonValueType{
			ScalarType:  typed.ScalarType,
			Dimension:   typed.Dimension,
			IsEmbedding: typed.IsEmbedding,
			IsVector:    true,
		}
	}
}

func (wrapper jsonValueType) ToValueType() ValueType {
	if wrapper.IsVector {
		return VectorType{
			ScalarType:  wrapper.ScalarType,
			Dimension:   wrapper.Dimension,
			IsEmbedding: wrapper.IsEmbedding,
		}
	} else {
		return wrapper.ScalarType
	}
}

func SerializeType(t ValueType) string {
	var wrapper jsonValueType
	wrapper.FromValueType(t)
	bytes, err := json.Marshal(wrapper)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func DeserializeType(t string) (ValueType, error) {
	var wrapper jsonValueType
	if err := json.Unmarshal([]byte(t), &wrapper); err != nil {
		return nil, err
	}
	return wrapper.ToValueType(), nil
}

func (t VectorType) Scalar() ScalarType {
	return t.ScalarType
}

func (t VectorType) IsVector() bool {
	return true
}

func (t VectorType) Type() reflect.Type {
	scalar := t.Scalar().Type()
	if scalar.Kind() == reflect.Ptr {
		scalar = scalar.Elem()
	}
	return reflect.SliceOf(scalar)
}

func (t VectorType) String() string {
	return fmt.Sprintf("%s[%d](embedding=%v)", t.Scalar().String(), t.Dimension, t.IsEmbedding)
}

func (t VectorType) ToProto() *pb.ValueType {
	scalarEnum, err := t.Scalar().ToProtoEnum()
	if err != nil {
		panic(err)
	}
	return &pb.ValueType{
		Type: &pb.ValueType_Vector{
			Vector: &pb.VectorType{
				Scalar:      scalarEnum,
				Dimension:   t.Dimension,
				IsEmbedding: t.IsEmbedding,
			},
		},
	}
}

type ScalarType string

func (t ScalarType) Scalar() ScalarType {
	return t
}

func (t ScalarType) IsVector() bool {
	return false
}

func (t ScalarType) String() string {
	return string(t)
}

// This method is used in encoding our supported data types to parquet.
// It returns a pointer type for scalar values to allow for nullability.
func (t ScalarType) Type() reflect.Type {
	switch t {
	case Int:
		return reflect.PointerTo(reflect.TypeOf(int(0)))
	case Int8:
		return reflect.PointerTo(reflect.TypeOf(int8(0)))
	case Int16:
		return reflect.PointerTo(reflect.TypeOf(int16(0)))
	case Int32:
		return reflect.PointerTo(reflect.TypeOf(int32(0)))
	case Int64:
		return reflect.PointerTo(reflect.TypeOf(int64(0)))
	case UInt8:
		return reflect.PointerTo(reflect.TypeOf(uint8(0)))
	case UInt16:
		return reflect.PointerTo(reflect.TypeOf(uint16(0)))
	case UInt32:
		return reflect.PointerTo(reflect.TypeOf(uint32(0)))
	case UInt64:
		return reflect.PointerTo(reflect.TypeOf(uint64(0)))
	case Float32:
		return reflect.PointerTo(reflect.TypeOf(float32(0)))
	case Float64:
		return reflect.PointerTo(reflect.TypeOf(float64(0)))
	case String:
		return reflect.PointerTo(reflect.TypeOf(string("")))
	case Bool:
		return reflect.PointerTo(reflect.TypeOf(bool(false)))
	case Timestamp:
		return reflect.TypeOf(time.Time{})
	case Datetime:
		return reflect.TypeOf(time.Time{})
	default:
		return nil
	}
}

func (t ScalarType) ToProto() *pb.ValueType {
	protoEnum, err := t.ToProtoEnum()
	if err != nil {
		panic(err)
	}
	return &pb.ValueType{
		Type: &pb.ValueType_Scalar{protoEnum},
	}
}

func (t ScalarType) ToProtoEnum() (pb.ScalarType, error) {
	protoVal, mapped := scalarToProto[t]
	if !mapped {
		errMsg := "ScalarType not mapped to proto: %s\nMap %v\n"
		err := fferr.NewInternalErrorf(errMsg, t, scalarToProto)
		return pb.ScalarType_NULL, err
	}
	return protoVal, nil
}

// TODO(simba) merge this into the serialize/deserialize above
type ValueTypeJSONWrapper struct {
	ValueType
}

func (vt *ValueTypeJSONWrapper) UnmarshalJSON(data []byte) error {
	v := map[string]VectorType{"ValueType": {}}
	if err := json.Unmarshal(data, &v); err == nil {
		vt.ValueType = v["ValueType"]
		return nil
	}

	s := map[string]ScalarType{"ValueType": ScalarType("")}
	if err := json.Unmarshal(data, &s); err == nil {
		vt.ValueType = s["ValueType"]
		return nil
	}

	return fferr.NewInternalError(fmt.Errorf("could not unmarshal value type: %v", data))
}

func (vt ValueTypeJSONWrapper) MarshalJSON() ([]byte, error) {
	switch vt.ValueType.(type) {
	case VectorType:
		return json.Marshal(map[string]VectorType{"ValueType": vt.ValueType.(VectorType)})
	case ScalarType:
		return json.Marshal(map[string]ScalarType{"ValueType": vt.ValueType.(ScalarType)})
	default:
		return nil, fferr.NewInternalError(fmt.Errorf("could not marshal value type: %v", vt.ValueType))
	}
}

type ColumnSchema struct {
	Name       ColumnName
	NativeType NativeType
	Type       ValueType
}

type Value struct {
	NativeType NativeType
	Type       ValueType
	Value      any
}

type NativeType string
type ColumnName string

type NativeToValueTypeMapper map[NativeType]ValueType

type TypeConverter func(interface{}) (interface{}, error)

type TypeConverterMapping map[string]TypeConverter

type Schema struct {
	Fields []ColumnSchema
	// todo: can include more state or behavior, etc.

	columnSanitizer func(string) string
}

// ColumnNames returns a slice of all column names in the schema
func (s Schema) ColumnNames() []string {
	names := make([]string, len(s.Fields))
	for i, field := range s.Fields {
		names[i] = string(field.Name)
	}
	return names
}

func (s Schema) SanitizedColumnNames() []string {
	names := make([]string, len(s.Fields))
	for i, field := range s.Fields {
		if s.columnSanitizer == nil {
			names[i] = sanitizeColumnName(string(field.Name))
		} else {
			names[i] = s.columnSanitizer(string(field.Name))

		}
	}
	return names
}

func sanitizeColumnName(name string) string {
	return db.Identifier{name}.Sanitize()
}
