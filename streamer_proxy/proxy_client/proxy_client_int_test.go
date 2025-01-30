package proxy_client

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type flightServer struct {
	flight.BaseFlightServer
	Records     map[string][]arrow.Record
	RecordNames []string
}

func (f *flightServer) DoGet(ticket *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	ticketData := string(ticket.GetTicket())
	fmt.Println("received ticket data: ", ticketData) // todox: use logger
	recordSlice, ok := f.Records["students"]
	if !ok {
		return status.Error(codes.NotFound, "flight not found")
	}

	writer := flight.NewRecordWriter(fs, ipc.WithSchema(recordSlice[0].Schema()))
	for _, r := range recordSlice {
		writer.Write(r)
	}

	return nil
}

func createStudentRecords() []arrow.Record {
	memoryAlloc := memory.NewGoAllocator()

	arrowMeta := arrow.NewMetadata(
		[]string{"randomKey1", "randomKey2"},
		[]string{"randomeValue1", "randomValue2"},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "graduated", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "ages", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "names", Type: arrow.BinaryTypes.String, Nullable: true},
		}, &arrowMeta,
	)

	mask := []bool{true, true, true, true, false} // one is masked
	chunks := [][]arrow.Array{
		{
			arrayFrom(memoryAlloc, []bool{true, false, true, false, true}, mask),
			arrayFrom(memoryAlloc, []int32{20, 43, 60, 18, 25}, mask),
			arrayFrom(memoryAlloc, []string{"neo", "asma", "derek", "tony", "bill"}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

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

	default:
		panic(fmt.Errorf("arrdata: invalid data slice type %T", a))
	}
}

func TestFlightServer_GetStreamProxyClient(t *testing.T) {
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
		Records:     map[string][]arrow.Record{"students": createStudentRecords()},
		RecordNames: []string{"students"},
	}
	// todo: make the records part of the flight server?
	flight.RegisterFlightServiceServer(grpcServer, &f)
	done := make(chan struct{})
	go func() {
		servErr := grpcServer.Serve(listener)
		if servErr != nil {
			fmt.Println("Server shutdown with an error: ", servErr)
		}
		close(done)
	}()

	defer func() {
		grpcServer.Stop()
		<-done
	}()

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

func TestFlightServer_MultipleRecordBatches2(t *testing.T) {
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
			memoryAlloc := memory.NewGoAllocator()
			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "batch_id", Type: arrow.PrimitiveTypes.Int32},
				}, nil,
			)

			// create the chunks
			chunks := make([][]arrow.Array, len(tt.batches))
			for i, batch := range tt.batches {
				chunks[i] = []arrow.Array{arrayFrom(memoryAlloc, batch, nil)}
			}

			recordSlice := make([]arrow.Record, len(chunks))
			for i, chunk := range chunks {
				recordSlice[i] = array.NewRecord(schema, chunk, -1)
			}

			// start the server
			grpcServer := grpc.NewServer()
			listener, listenErr := net.Listen("tcp", "localhost:0")
			if listenErr != nil {
				t.Fatalf("Failed to bind address: %s", listenErr)
			}

			// prep the env
			testPort := listener.Addr().(*net.TCPAddr).Port

			t.Setenv("ICEBERG_PROXY_HOST", "localhost")
			t.Setenv("ICEBERG_PROXY_PORT", fmt.Sprintf("%d", testPort))

			f := flightServer{
				Records:     map[string][]arrow.Record{"students": recordSlice},
				RecordNames: []string{"students"},
			}
			flight.RegisterFlightServiceServer(grpcServer, &f)
			done := make(chan struct{})
			go func() {
				servErr := grpcServer.Serve(listener)
				if servErr != nil {
					fmt.Println("Server shutdown with an error: ", servErr)
				}
				close(done)
			}()
			defer func() {
				grpcServer.Stop()
				<-done
			}()

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
