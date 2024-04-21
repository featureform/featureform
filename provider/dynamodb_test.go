package provider

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	
	vt "github.com/featureform/provider/types"
)

func TestOnlineStoreDynamoDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	store := GetTestingDynamoDB(t)
	test := OnlineStoreTest{
		t:            t,
		store:        store,
		testNil:      true,
		testFloatVec: true,
		testBatch:    true,
	}
	test.Run()
}

func TestParsingTableMetadata(t *testing.T) {
	vecType := vt.VectorType{vt.Float32, 128, true}
	successCases := map[dynamodbMetadataEntry]*dynamodbTableMetadata{
		{"test1", vt.SerializeType(vt.Float32), int(serializeV0)}: {vt.Float32, serializeV0},
		{"test2", vt.SerializeType(vecType), int(serializeV1)}: {vecType, serializeV1},
	}
	errorCases := map[string]*dynamodbMetadataEntry{
		"Unknown type":              {"a", "unknown_type", 0},
		"Unknown serialize version": {"b", vt.SerializeType(vt.Float32), 13371235},
	}
	for test, expected := range successCases {
		t.Run(test.Tablename, func(t *testing.T) {
			found, err := test.ToTableMetadata()
			if err != nil {
				t.Fatalf("Failed to serialized: %v\n%s\n", test, err)
			}
			if !reflect.DeepEqual(found, expected) {
				t.Fatalf("Table metadata did not match\nFound: %v\nExpected: %v\n", found, expected)
			}
		})
	}
	for testName, test := range errorCases {
		t.Run(testName, func(t *testing.T) {
			if _, err := test.ToTableMetadata(); err == nil {
				t.Fatalf("Succeeded to serialized %v", test)
			}
		})
	}
}

func TestDynamoSerializers(t *testing.T) {
	type testCases map[vt.ValueType]any
	simpleTests := testCases{
		vt.NilType: nil,
		vt.Int:     123,
		vt.Int32:   int32(1),
		vt.Int64:   int64(1),
		vt.Float32: float32(12.3),
		vt.Float64: float64(45.6),
		vt.String:  "apple",
		vt.Bool:    true,
	}
	allSerializers := make([]serializeVersion, 0, len(serializers))
	for ver, _ := range serializers {
		allSerializers = append(allSerializers, ver)
	}
	timestamp := time.Now().UTC().Truncate(time.Second)
	date := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)
	timeTests := testCases{
		vt.Timestamp: timestamp,
		vt.Datetime:  date,
	}
	timeSerializers := []serializeVersion{serializeV1}
	uintTests := testCases{
		vt.UInt8:  uint8(1),
		vt.UInt16: uint16(1),
		vt.UInt32: uint32(0xff),
		vt.UInt64: uint64(0xffff),
	}
	uintSerializers := []serializeVersion{}
	smallBitTests := testCases{
		vt.Int8:  int8(1),
		vt.Int16: int16(1),
	}
	smallBitSerializers := []serializeVersion{}
	nilTests := testCases{
		vt.NilType:                       nil,
		vt.Int:                           nil,
		vt.Int32:                         nil,
		vt.Int64:                         nil,
		vt.Float32:                       nil,
		vt.Float64:                       nil,
		vt.String:                        nil,
		vt.Bool:                          nil,
		vt.VectorType{vt.Float32, 1, false}: nil,
	}
	nilSerializers := allSerializers

	testSerializer := func(t *testing.T, serializer serializer, typ vt.ValueType, val any) {
		serial, err := serializer.Serialize(typ, val)
		if err != nil {
			t.Fatalf("Failed to serialize: %s %v\n%s\n", typ, val, err)
		}
		found, err := serializer.Deserialize(typ, serial)
		if err != nil {
			t.Fatalf("Failed to deserialize: %s %v\nDynamo Val: %v\n%s\n", typ, val, serial, err)
		}
		if !reflect.DeepEqual(found, val) {
			t.Fatalf("Value not equal\nFound: %v\n Expected: %v\nSerial: %v\n", found, val, serial)
		}
	}

	runTestCases := func(t *testing.T, vers []serializeVersion, tests testCases) {
		for _, version := range vers {
			serializer := serializers[version]
			t.Run(fmt.Sprintf("Version %d", version), func(t *testing.T) {
				for typ, val := range tests {
					t.Run(typ.String(), func(t *testing.T) {
						testSerializer(t, serializer, typ, val)
					})
				}
			})
		}
	}
	runTestCases(t, allSerializers, simpleTests)
	runTestCases(t, timeSerializers, timeTests)
	runTestCases(t, uintSerializers, uintTests)
	runTestCases(t, smallBitSerializers, smallBitTests)
	runTestCases(t, nilSerializers, nilTests)
}

func TestDynamoTimeFormatsV1(t *testing.T) {
	serializer := serializers[serializeV1]
	expected := time.Unix(1, 0).UTC()
	differentForms := map[string]any{
		"unix int string":  "1",
		"unix int":         1,
		"timestamp string": expected.Format(time.RFC850),
	}
	testTS := func(t *testing.T, val any) {
		types := []vt.ValueType{vt.Timestamp, vt.Datetime}
		for _, typ := range types {
			serial, err := serializer.Serialize(typ, val)
			if err != nil {
				t.Fatalf("Failed to serialize: %s %v\n%s\n", typ, val, err)
			}
			found, err := serializer.Deserialize(typ, serial)
			if err != nil {
				t.Fatalf("Failed to deserialize: %s %v\nDynamo Val: %v\n%s\n", typ, val, serial, err)
			}
			if !found.(time.Time).Equal(expected) {
				t.Fatalf("Value not equal\nFound: %v\n Expected: %v\nSerial: %v\n", found, expected, serial)
			}
		}
	}
	for name, form := range differentForms {
		t.Run(name, func(t *testing.T) {
			testTS(t, form)
		})
	}
}

func TestDynamoBoolFormatsV1(t *testing.T) {
	serializer := serializers[serializeV1]
	expected := true
	differentForms := map[string]any{
		"bool int string": "1",
		"bool int":        1,
		"bool string":     "true",
	}
	testTS := func(t *testing.T, val any) {
		typ := vt.Bool
		serial, err := serializer.Serialize(typ, val)
		if err != nil {
			t.Fatalf("Failed to serialize: %s %v\n%s\n", typ, val, err)
		}
		found, err := serializer.Deserialize(typ, serial)
		if err != nil {
			t.Fatalf("Failed to deserialize: %s %v\nDynamo Val: %v\n%s\n", typ, val, serial, err)
		}
		if found.(bool) != expected {
			t.Fatalf("Value not equal\nFound: %v\n Expected: %v\nSerial: %v\n", found, expected, serial)
		}
	}
	for name, form := range differentForms {
		t.Run(name, func(t *testing.T) {
			testTS(t, form)
		})
	}
}

func TestDynamoNumericCasting(t *testing.T) {
	canonicalValues := map[vt.ValueType]any{
		vt.Float32: float32(1.0),
		vt.Float64: float64(1.0),
		vt.Int:     int(1),
		vt.Int32:   int32(1),
		vt.Int64:   int64(1),
	}
	possibleNumerics := []any{
		"1", "1.0", int8(1), int16(1), int32(1), int64(1), int(1), float32(1), float64(1),
	}
	serializer := serializers[serializeV1]

	testNumeric := func(t *testing.T, typ vt.ValueType, val any, expected any) {
		serial, err := serializer.Serialize(typ, val)
		if err != nil {
			t.Fatalf("Failed to serialize: %s %v\n%s\n", typ, val, err)
		}
		found, err := serializer.Deserialize(typ, serial)
		if err != nil {
			t.Fatalf("Failed to deserialize: %s %v\nDynamo Val: %v\n%s\n", typ, val, serial, err)
		}
		if !reflect.DeepEqual(found, expected) {
			t.Fatalf("Value not equal\nFound: %v\n Expected: %v\nSerial: %v\n", found, val, serial)
		}
	}
	for typ, expected := range canonicalValues {
		t.Run(typ.String(), func(t *testing.T) {
			for _, numeric := range possibleNumerics {
				testNumeric(t, typ, numeric, expected)
			}
		})
	}
}

func TestFailSerializeV1(t *testing.T) {
	type testCase struct {
		vt  vt.ValueType
		val any
	}
	tests := []testCase{
		{vt.Float32, "abc"},
		{vt.Float64, "abc"},
		{vt.Int, "abc"},
		{vt.Int32, "abc"},
		{vt.Int64, "abc"},
		{vt.Float32, []float32{1.0}},
		{vt.Float64, []float64{1.2}},
		{vt.Int, []int{1}},
		{vt.Int32, []int32{1}},
		{vt.Int64, []int64{1}},
		{vt.Bool, "not"},
		{vt.String, true},
		{vt.Timestamp, true},
		{vt.Timestamp, "123/23/2033"},
		{vt.VectorType{vt.Float32, 1, false}, []string{"abc"}},
		{vt.VectorType{vt.Float32, 1, false}, []float32{1, 2}},
		{vt.VectorType{vt.Float32, 1, false}, float32(1.0)},
	}
	serializer := serializers[serializeV1]
	for _, test := range tests {
		testName := fmt.Sprintf("%s_%v", test.vt.String(), test.val)
		t.Run(testName, func(t *testing.T) {
			serial, err := serializer.Serialize(test.vt, test.val)
			if err == nil {
				t.Fatalf("Succeeded to serialize: %v as %s\nFound: %v\n", test.val, test.vt.String(), serial)
			}
		})
	}
}

func TestFailDeserializeV1(t *testing.T) {
	emptyList := &types.AttributeValueMemberL{Value: nil}
	unsupported := &types.AttributeValueMemberB{Value: nil}
	wrongNumFormat := &types.AttributeValueMemberN{Value: "not an int"}
	stringList := &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "abc"}}}
	numList := &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberN{Value: "1"}}}
	mixedList := &types.AttributeValueMemberL{
		Value: []types.AttributeValue{&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberS{Value: "abc"}},
	}
	unknownType := vt.ScalarType("Unknown")
	type testCase struct {
		vt  vt.ValueType
		val types.AttributeValue
	}
	tests := map[string]testCase{
		"Unknown type":           {unknownType, unsupported},
		"Float32 wrong":          {vt.Float32, emptyList},
		"Float64 wrong":          {vt.Float64, emptyList},
		"Int wrong":              {vt.Int, emptyList},
		"Int32 wrong":            {vt.Int32, emptyList},
		"Int64 wrong":            {vt.Int64, emptyList},
		"Timestamp wrong":        {vt.Timestamp, emptyList},
		"Float32 wrong format":   {vt.Float32, wrongNumFormat},
		"Float64 wrong format":   {vt.Float64, wrongNumFormat},
		"Int wrong format":       {vt.Int, wrongNumFormat},
		"Int32 wrong format":     {vt.Int32, wrongNumFormat},
		"Int64 wrong format":     {vt.Int64, wrongNumFormat},
		"Timestamp wrong format": {vt.Timestamp, wrongNumFormat},
		"Bool wrong":             {vt.Bool, emptyList},
		"String wrong":           {vt.String, emptyList},
		"Vec wrong":              {vt.VectorType{vt.Float32, 1, false}, unsupported},
		"Vec unknown type":       {vt.VectorType{unknownType, 1, false}, unsupported},
		"FloatVec wrong size":    {vt.VectorType{vt.Float32, 2, false}, numList},
		"FloatVec type":          {vt.VectorType{vt.Float32, 1, false}, stringList},
		"FloatVec mixed":         {vt.VectorType{vt.Float32, 2, false}, mixedList},
		"StringVec size":         {vt.VectorType{vt.String, 2, false}, stringList},
		"StringVec type":         {vt.VectorType{vt.String, 1, false}, numList},
		"StringVec mixed":        {vt.VectorType{vt.String, 2, false}, mixedList},
	}
	serializer := serializers[serializeV1]
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			found, err := serializer.Deserialize(test.vt, test.val)
			if err == nil {
				t.Fatalf("Succeeded to deserialize\nFound: %v\n", found)
			}
		})
	}
}
