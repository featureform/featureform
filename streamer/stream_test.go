package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/apache/arrow/go/v15/arrow/flight"
	pb "github.com/featureform/streamer_proxy/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// todo: these tests are really just sanity checkers

func TestStreamData_Long(t *testing.T) {
	serverAddress := "localhost:8085"
	tableName := "table_data_long"
	insecureCreds := grpc.WithTransportCredentials(insecure.NewCredentials())

	// initial grcp connection
	conn, err := grpc.NewClient(serverAddress, insecureCreds)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// wrap the grpc with a flight client
	client := flight.NewFlightServiceClient(conn)
	if err != nil {
		log.Fatalf("Failed to connect to Flight server: %v", err)
	}

	// create the flight ticket
	ticket := &flight.Ticket{
		Ticket: []byte(tableName),
	}

	flightData, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		t.Fatalf("Failed to fetch data: %v", err)
	}

	r, err := flight.NewRecordReader(flightData)
	if err != nil {
		t.Fatal("could no create the record reader", err)
	}

	var rowCount int64 = 0
	for {
		record, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Error(err)
		}
		rowCount += record.NumRows()
	}
	fmt.Println("total rowCount is: ", rowCount)
}

func TestStreamData_Short(t *testing.T) {
	serverAddress := "localhost:8085"
	tableName := "table_data_short" // uh oh
	insecureCreds := grpc.WithTransportCredentials(insecure.NewCredentials())

	// initial grcp connection
	conn, err := grpc.NewClient(serverAddress, insecureCreds)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// wrap the grpc with a flight client
	client := flight.NewFlightServiceClient(conn)
	if err != nil {
		log.Fatalf("Failed to connect to Flight server: %v", err)
	}

	// create the flight ticket
	ticket := &flight.Ticket{
		Ticket: []byte(tableName),
	}

	flightData, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		t.Fatalf("Failed to fetch data: %v", err)
	}

	recordBatch, err := flight.NewRecordReader(flightData)
	if err != nil {
		t.Fatal("could no create the record reader", err)
	}

	fmt.Println("schema:")
	fmt.Println(recordBatch.Schema())

	var rowCount int64 = 0
	for recordBatch.Next() {
		// todo: retain or release? each next call dumps the memory
		record := recordBatch.Record()
		rowCount += record.NumRows()
		for i := 0; i < int(record.NumRows()); i++ {
			row := []any{}
			for j := 0; j < int(record.NumCols()); j++ {
				column := record.Column(j)
				row = append(row, column.ValueStr(i))
			}
			fmt.Printf("Row %d: %v \n", i, row)
		}
	}
	fmt.Println("total rowCount is:", rowCount)
}

func TestStreamData_FromGoProxy(t *testing.T) {
	serverAddress := "localhost:8087" // the go-proxy address (NOT the same as the python-streamer)
	tableName := "table_data_long"
	insecureCreds := grpc.WithTransportCredentials(insecure.NewCredentials())

	// Initial gRPC connection
	conn, err := grpc.NewClient(serverAddress, insecureCreds)
	if err != nil {
		log.Fatalf("Failed to connect to Go Proxy: %v", err)
	}
	defer conn.Close()

	client := pb.NewGoProxyClient(conn)

	stream, err := client.StreamData(context.TODO(), &pb.StreamRequest{
		TableName: tableName,
	})

	if err != nil {
		t.Fatalf("Call to stream data failed: %v", err)
	}

	//run through the stream bytes
	readCount := 0
	for {
		batch, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error receiving bytes: %v", err)
		}
		readCount++
		// every client can be arrow flight agnostic or use the lib as needed!
		log.Println("Received pb.RecordBatch")
		log.Printf("Header: %v\n", batch.GetDataHeader())
		log.Printf("Body: %v\n", batch.GetDataBody())
		log.Printf("AppMeta: %v\n", batch.GetAppMetadata())
	}

	log.Println("Total passes: ", readCount)

}