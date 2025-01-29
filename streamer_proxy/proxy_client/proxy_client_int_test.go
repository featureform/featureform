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

var (
	Records     = make(map[string][]arrow.Record)
	RecordNames []string
)

type flightServer struct {
	flight.BaseFlightServer
}

func (f *flightServer) DoGet(ticket *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	ticketData := string(ticket.GetTicket())
	fmt.Println("received ticket data: ", ticketData) // todox: use logger
	recordSlice, ok := Records["students"]
	if !ok {
		return status.Error(codes.NotFound, "flight not found")
	}

	writer := flight.NewRecordWriter(fs, ipc.WithSchema(recordSlice[0].Schema()))
	for _, r := range recordSlice {
		writer.Write(r)
	}

	return nil
}

func init() {
	Records["students"] = createStudentRecords()

	for k := range Records {
		RecordNames = append(RecordNames, k)
	}
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
	// prep flight server
	grpcServer := grpc.NewServer()
	listener, listenErr := net.Listen("tcp", "localhost:9191") // todox: need to update port dynamically
	if listenErr != nil {
		t.Fatalf("Failed to bind address to :%s", listenErr)
	}

	// start the proxy flight server
	f := flightServer{}
	// todo: make the records part of the flight server?
	flight.RegisterFlightServiceServer(grpcServer, &f)
	go func() {
		servErr := grpcServer.Serve(listener)
		if servErr != nil {
			fmt.Println("oops")
			panic("server died")
		}
	}()

	defer grpcServer.Stop()

	t.Setenv("ICEBERG_PROXY_HOST", "localhost")
	t.Setenv("ICEBERG_PROXY_PORT", "9191")

	// get proxy client
	proxyClient, proxyErr := GetStreamProxyClient(context.Background(), "some_name", "some_variant", -1)
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
	assert.False(t, proxyClient.Next(), "Subsequent call to next() should be false")

}
