package provider

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestOnlineStoreDynamoDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	dynamoAccessKey, ok := os.LookupEnv("DYNAMO_ACCESS_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_ACCESS_KEY variable")
	}
	dynamoSecretKey, ok := os.LookupEnv("DYNAMO_SECRET_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_SECRET_KEY variable")
	}
	endpoint := os.Getenv("DYNAMO_ENDPOINT")
	dynamoConfig := &pc.DynamodbConfig{
		Region:    "us-east-1",
		AccessKey: dynamoAccessKey,
		SecretKey: dynamoSecretKey,
		Endpoint:  endpoint,
	}

	store, err := GetOnlineStore(pt.DynamoDBOnline, dynamoConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:            t,
		store:        store,
		testNil:      true,
		testFloatVec: true,
	}
	test.Run()
}

func TestParsingTableMetadata(t *testing.T) {
	vecType := VectorType{Float32, 128, true}
	successCases := map[dynamodbMetadataEntry]*dynamodbTableMetadata{
		{"test1", serializeType(Float32), int(serializeV0)}: {Float32, serializeV0},
		{"test2", serializeType(vecType), int(serializeV1)}: {vecType, serializeV1},
	}
	errorCases := map[string]*dynamodbMetadataEntry{
		"Unknown type":              {"a", "unknown_type", 0},
		"Unknown serialize version": {"b", serializeType(Float32), 13371235},
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
	type testCases map[ValueType]any
	simpleTests := testCases{
		NilType: nil,
		Int:     123,
		Int32:   int32(1),
		Int64:   int64(1),
		Float32: float32(12.3),
		Float64: float64(45.6),
		String:  "apple",
		Bool:    true,
	}
	allSerializers := make([]serializeVersion, 0, len(serializers))
	for ver, _ := range serializers {
		allSerializers = append(allSerializers, ver)
	}
	timestamp := time.Now().UTC().Truncate(time.Second)
	date := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)
	timeTests := testCases{
		Timestamp: timestamp,
		Datetime:  date,
	}
	timeSerializers := []serializeVersion{serializeV1}
	uintTests := testCases{
		UInt8:  uint8(1),
		UInt16: uint16(1),
		UInt32: uint32(0xff),
		UInt64: uint64(0xffff),
	}
	uintSerializers := []serializeVersion{}
	smallBitTests := testCases{
		Int8:  int8(1),
		Int16: int16(1),
	}
	smallBitSerializers := []serializeVersion{}
	nilTests := testCases{
		NilType:                       nil,
		Int:                           nil,
		Int32:                         nil,
		Int64:                         nil,
		Float32:                       nil,
		Float64:                       nil,
		String:                        nil,
		Bool:                          nil,
		VectorType{Float32, 1, false}: nil,
	}
	nilSerializers := allSerializers

	testSerializer := func(t *testing.T, serializer serializer, typ ValueType, val any) {
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
		types := []ValueType{Timestamp, Datetime}
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
		typ := Bool
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
	canonicalValues := map[ValueType]any{
		Float32: float32(1.0),
		Float64: float64(1.0),
		Int:     int(1),
		Int32:   int32(1),
		Int64:   int64(1),
	}
	possibleNumerics := []any{
		"1", "1.0", int8(1), int16(1), int32(1), int64(1), int(1), float32(1), float64(1),
	}
	serializer := serializers[serializeV1]

	testNumeric := func(t *testing.T, typ ValueType, val any, expected any) {
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
		vt  ValueType
		val any
	}
	tests := []testCase{
		{Float32, "abc"},
		{Float64, "abc"},
		{Int, "abc"},
		{Int32, "abc"},
		{Int64, "abc"},
		{Float32, []float32{1.0}},
		{Float64, []float64{1.2}},
		{Int, []int{1}},
		{Int32, []int32{1}},
		{Int64, []int64{1}},
		{Bool, "not"},
		{String, true},
		{Timestamp, true},
		{Timestamp, "123/23/2033"},
		{VectorType{Float32, 1, false}, []string{"abc"}},
		{VectorType{Float32, 1, false}, []float32{1, 2}},
		{VectorType{Float32, 1, false}, float32(1.0)},
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
	unknownType := ScalarType("Unknown")
	type testCase struct {
		vt  ValueType
		val types.AttributeValue
	}
	tests := map[string]testCase{
		"Unknown type":           {unknownType, unsupported},
		"Float32 wrong":          {Float32, emptyList},
		"Float64 wrong":          {Float64, emptyList},
		"Int wrong":              {Int, emptyList},
		"Int32 wrong":            {Int32, emptyList},
		"Int64 wrong":            {Int64, emptyList},
		"Timestamp wrong":        {Timestamp, emptyList},
		"Float32 wrong format":   {Float32, wrongNumFormat},
		"Float64 wrong format":   {Float64, wrongNumFormat},
		"Int wrong format":       {Int, wrongNumFormat},
		"Int32 wrong format":     {Int32, wrongNumFormat},
		"Int64 wrong format":     {Int64, wrongNumFormat},
		"Timestamp wrong format": {Timestamp, wrongNumFormat},
		"Bool wrong":             {Bool, emptyList},
		"String wrong":           {String, emptyList},
		"Vec wrong":              {VectorType{Float32, 1, false}, unsupported},
		"Vec unknown type":       {VectorType{unknownType, 1, false}, unsupported},
		"FloatVec wrong size":    {VectorType{Float32, 2, false}, numList},
		"FloatVec type":          {VectorType{Float32, 1, false}, stringList},
		"FloatVec mixed":         {VectorType{Float32, 2, false}, mixedList},
		"StringVec size":         {VectorType{String, 2, false}, stringList},
		"StringVec type":         {VectorType{String, 1, false}, numList},
		"StringVec mixed":        {VectorType{String, 2, false}, mixedList},
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
