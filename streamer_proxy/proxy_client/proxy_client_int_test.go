package proxy_client

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/featureform/logging"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type flightServer struct {
	flight.BaseFlightServer
	Records []arrow.Record
	Schema  arrow.Schema
	Logger  logging.Logger
}

func startProxyServer(t *testing.T, recordSlice []arrow.Record, schema *arrow.Schema) (func(), error) {
	// prep flight server and env
	grpcServer := grpc.NewServer()
	listener, listenErr := net.Listen("tcp", "localhost:0")
	if listenErr != nil {
		t.Fatalf("Failed to bind address to :%s", listenErr)
	}

	// pull the os assigned port
	testPort := listener.Addr().(*net.TCPAddr).Port

	t.Setenv("ICEBERG_PROXY_HOST", "localhost")
	t.Setenv("ICEBERG_PROXY_PORT", fmt.Sprintf("%d", testPort))

	// start the proxy flight server
	f := flightServer{
		Records: recordSlice,
		Schema:  *schema,
		Logger:  logging.NewTestLogger(t),
	}

	flight.RegisterFlightServiceServer(grpcServer, &f)
	done := make(chan struct{})
	go func() {
		t.Logf("Test flight server starting on port: %d", testPort)
		servErr := grpcServer.Serve(listener)
		if servErr != nil {
			t.Logf("Test flight server shutdown with an error: %v", servErr)
		}
		close(done)
	}()

	cleanUp := func() {
		grpcServer.Stop()
		<-done
		t.Log("Test flight server stopped successfully")
	}
	return cleanUp, nil
}

func (f *flightServer) DoGet(ticket *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	ticketData := string(ticket.GetTicket())
	f.Logger.Infof("received ticket data: %v", ticketData)
	recordSlice := f.Records
	writer := flight.NewRecordWriter(fs, ipc.WithSchema(&f.Schema))
	for _, r := range recordSlice {
		writer.Write(r)
	}

	return nil
}

// helper function so each test can create its own records to store in the temp server
func createRecords(schema *arrow.Schema, metadata arrow.Metadata, mask []bool, data [][]interface{}) []arrow.Record {
	memoryAlloc := memory.NewGoAllocator()

	// create the metadata
	if metadata.Len() > 0 {
		schema = arrow.NewSchema(schema.Fields(), &metadata)
	}

	// convert data chunks into arrow arrays
	chunks := make([][]arrow.Array, len(data))
	for i, rowData := range data {
		columnArrays := make([]arrow.Array, len(rowData))
		for j, colData := range rowData {
			columnArrays[j] = arrayFrom(memoryAlloc, colData, mask)
		}
		chunks[i] = columnArrays
	}

	// add the records
	recordSlice := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recordSlice[i] = array.NewRecord(schema, chunk, -1)
	}

	return recordSlice
}

func arrayFrom(memoryAlloc memory.Allocator, a interface{}, valids []bool) arrow.Array {
	if memoryAlloc == nil {
		memoryAlloc = memory.NewGoAllocator()
	}

	switch a := a.(type) {
	case []bool:
		bldr := array.NewBooleanBuilder(memoryAlloc)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewBooleanArray()

	case []int32:
		bldr := array.NewInt32Builder(memoryAlloc)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt32Array()

	case []string:
		bldr := array.NewStringBuilder(memoryAlloc)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewStringArray()
	case []float64:
		bldr := array.NewFloat64Builder(memoryAlloc)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat64Array()

	default:
		panic(fmt.Errorf("arrdata: invalid data slice type %T", a))
	}
}

func TestClient_GetStreamProxyClient_Success(t *testing.T) {
	// define the schema, metadata, mask, and data chunks
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "graduated", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "ages", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "names", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil,
	)
	metadata := arrow.NewMetadata(
		[]string{"randomKey1", "randomKey2"},
		[]string{"randomValue1", "randomValue2"},
	)
	mask := []bool{true, true, true, true, false}
	data := [][]interface{}{
		{
			[]bool{true, false, true, false, true},
			[]int32{20, 43, 60, 18, 25},
			[]string{"neo", "asma", "derek", "tony", "bill"},
		},
	}
	recordSlice := createRecords(schema, metadata, mask, data)

	cleanUp, startErr := startProxyServer(t, recordSlice, schema)
	if startErr != nil {
		t.Fatalf("could not setup proxy server. error: %v", startErr)
	}
	defer cleanUp()

	// get proxy client
	proxyClient, proxyErr := GetStreamProxyClient(context.Background(), "some_name", "some_variant", 10)
	if proxyErr != nil {
		t.Fatalf("An error occurred calling GetStreamProxyClient(): %v", proxyErr)
	}
	initialNext := proxyClient.Next()

	assert.True(t, initialNext)
	assert.Len(t, proxyClient.Columns(), 3)
	assert.Equal(t, proxyClient.Columns(), []string{"graduated", "ages", "names"})

	values := proxyClient.Values()
	assert.Equal(t, values[0], []string{"true", "20", "neo"})
	assert.Equal(t, values[1], []string{"false", "43", "asma"})
	assert.Equal(t, values[2], []string{"true", "60", "derek"})
	assert.Equal(t, values[3], []string{"false", "18", "tony"})
	assert.False(t, proxyClient.Next(), "Final call to next() should be false")

}

func TestClient_MultipleRecordBatches(t *testing.T) {
	tests := []struct {
		name          string
		batches       [][]int32
		expectedRows  [][]string
		expectedCalls int
	}{
		{
			name: "Single batch of 3 records",
			batches: [][]int32{
				{1, 2, 3},
			},
			expectedRows: [][]string{
				{"1"}, {"2"}, {"3"},
			},
			expectedCalls: 1,
		},
		{
			name: "Double batch of 3 records",
			batches: [][]int32{
				{1, 2, 3},
				{4, 5, 6},
			},
			expectedRows: [][]string{
				{"1"}, {"2"}, {"3"},
				{"4"}, {"5"}, {"6"},
			},
			expectedCalls: 2,
		},
		{
			name: "Empty batch",
			batches: [][]int32{
				{},
			},
			expectedRows:  [][]string(nil),
			expectedCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "batch_id", Type: arrow.PrimitiveTypes.Int32},
				}, nil,
			)

			data := make([][]interface{}, len(tt.batches))
			for i, batch := range tt.batches {
				data[i] = []interface{}{batch}
			}

			recordSlice := createRecords(schema, arrow.Metadata{}, nil, data)

			cleanUp, startErr := startProxyServer(t, recordSlice, schema)
			if startErr != nil {
				t.Fatalf("could not setup proxy server. error: %v", startErr)
			}
			defer cleanUp()

			// get the proxy client
			proxyClient, proxyErr := GetStreamProxyClient(context.Background(), "some_name", "some_variant", 10)
			assert.NoError(t, proxyErr)

			var actualRows [][]string
			calls := 0
			for proxyClient.Next() {
				dataMatrix := proxyClient.Values()
				for _, dataRow := range dataMatrix {
					stringArray, ok := dataRow.([]string)
					if !ok {
						fmt.Println("The data row did not cast correctly")
						t.FailNow()
					}
					actualRows = append(actualRows, stringArray)
				}
				calls++ // increment the calls for the test
			}

			assert.Equal(t, tt.expectedRows, actualRows, "Retrieved rows should match expected rows")
			assert.Equal(t, tt.expectedCalls, calls, "Number of Next() calls should match expected count")
		})
	}
}

func TestClient_ConnectionFailure(t *testing.T) {
	t.Setenv("ICEBERG_PROXY_HOST", "localhost")
	t.Setenv("ICEBERG_PROXY_PORT", "9999") // Invalid port, with no server available to connect to

	someName, someVariant := "some_name", "some_variant"
	_, proxyErr := GetStreamProxyClient(context.Background(), someName, someVariant, 10)

	assert.Error(t, proxyErr, "Expected error when connecting to an invalid Flight server")
	assert.ErrorContainsf(t, proxyErr, fmt.Sprintf("failed to fetch data for source (%s) and variant (%s) from proxy", someName, someVariant), "")
}

func TestClient_SchemaMismatch(t *testing.T) {
	// define a "wrong" schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "wrong_field", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := [][]interface{}{
		{
			[]float64{1.1, 2.2, 3.3},
		},
	}

	recordSlice := createRecords(schema, arrow.Metadata{}, nil, data)

	cleanUp, startErr := startProxyServer(t, recordSlice, schema)
	if startErr != nil {
		t.Fatalf("could not setup proxy server. error: %v", startErr)
	}
	defer cleanUp()

	proxyClient, proxyErr := GetStreamProxyClient(context.Background(), "some_name", "some_variant", 10)
	assert.NoError(t, proxyErr)
	assert.NotEqual(t, proxyClient.Columns(), []string{"batch_id"}, "Schema should not match")
}

func TestClient_LargeData_StressTest(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "big_data", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	dataSize := int32(20_000)
	largeData := make([]int32, dataSize)
	for i := int32(0); i < dataSize; i++ {
		largeData[i] = i
	}

	recordSlice := createRecords(schema, arrow.Metadata{}, nil, [][]interface{}{{largeData}})

	cleanUp, startErr := startProxyServer(t, recordSlice, schema)
	if startErr != nil {
		t.Fatalf("could not setup proxy server. error: %v", startErr)
	}
	defer cleanUp()

	start := time.Now()
	proxyClient, proxyErr := GetStreamProxyClient(context.Background(), "some_name", "some_variant", 10)
	assert.NoError(t, proxyErr)

	count := 0
	for proxyClient.Next() {
		count += len(proxyClient.Values())
	}

	elapsed := time.Since(start)
	t.Logf("TestClient_LargeData_StressTest() took %s to process %d rows", elapsed, count)
	assert.Equal(t, int(dataSize), count, "Large dataset should be read completely, the client returned missing data in the stream!")
}

func TestClient_EmptyData(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "empty_field", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	// no data to return
	recordSlice := createRecords(schema, arrow.Metadata{}, nil, [][]interface{}{})

	cleanUp, startErr := startProxyServer(t, recordSlice, schema)
	if startErr != nil {
		t.Fatalf("could not setup proxy server. error: %v", startErr)
	}
	defer cleanUp()

	someName, someVariant := "some_name", "some_variant"
	_, proxyErr := GetStreamProxyClient(context.Background(), someName, someVariant, 10)

	assert.Error(t, proxyErr, "Expected error when connecting to an invalid Flight server")
	assert.ErrorContainsf(t, proxyErr, fmt.Sprintf("connection established, but no data available for source (%s) and variant (%s)", someName, someVariant), "")
}
