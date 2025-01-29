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
	fmt.Println("received ticket data: ", ticketData) // todox: delete
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
	fmt.Println("asdf")
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

func TestFlightServer(t *testing.T) {
	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", "localhost:9191") // todox: need to update port dynamically
	if err != nil {
		t.Fatalf("Failed to bind address to :%s", err)
	}

	// start the proxy flight server
	f := flightServer{}
	flight.RegisterFlightServiceServer(grpcServer, &f)
	go func() {
		servErr := grpcServer.Serve(listener)
		if servErr != nil {
			fmt.Println("oops")
			panic("server died")
		}
	}()

	defer grpcServer.Stop()

	fmt.Println(len(Records))

	t.Setenv("ICEBERG_PROXY_HOST", "localhost")
	t.Setenv("ICEBERG_PROXY_PORT", "9191")

	iterator, err := GetStreamProxyClient(context.TODO(), "some", "some", 100)

	// todox: delete this
	fmt.Println(iterator.Columns())
	fmt.Println(iterator.Next())
	fmt.Println(iterator.Values())

	// todox: need to assert the returned columns and values.
	// also include the schema in the proxy client?

	if err != nil {
		fmt.Println("explode")
		panic("haha")
	}
}
