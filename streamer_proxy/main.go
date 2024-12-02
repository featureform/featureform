package main

import (
	"context"
	"log"
	"net"

	"github.com/apache/arrow/go/v14/arrow/flight"
	pb "github.com/featureform/streamer_proxy/proto"
	"google.golang.org/grpc"
)

// gRPC service implementation for Go Proxy
// note: this could change completely depending on a few factors like required inputs, health checks, etc.
type GoProxyServer struct {
	pb.UnimplementedGoProxyServer
	flightClient flight.Client
}

// StreamData is the gRPC method exposed to internal services
func (gps *GoProxyServer) StreamData(ctx context.Context, req *pb.StreamRequest) (*pb.Empty, error) {
	// Connect to the Python Streamer
	ticket := &flight.Ticket{
		Ticket: []byte(req.TableName),
	}
	stream, err := gps.flightClient.DoGet(ctx, ticket)
	if err != nil {
		log.Printf("Failed to fetch data from Python Streamer: %v\n", err)
		return nil, err
	}

	// Handle the received data stream
	for {
		record, err := stream.Recv()
		if err != nil {
			log.Printf("End of stream or error: %v\n", err)
			break
		}
		log.Printf("Received data: %v\n", record)
	}

	// todox: or we could refactor to just pass back the raw response
	// and let the client caller handle the processsing.
	// can also pass in a closure to process items with and store the results locally
	return &pb.Empty{}, nil
}

// NewGoProxyServer initializes the proxy server with a connection to the Python Streamer
func NewGoProxyServer(streamerAddr string) (*GoProxyServer, error) {
	client, err := flight.NewClientWithMiddleware(streamerAddr, nil, nil)
	if err != nil {
		return nil, err
	}

	return &GoProxyServer{
		flightClient: client,
	}, nil
}

func main() {
	streamerAddr := "grpc://python-streamer:8085"

	server, err := NewGoProxyServer(streamerAddr)
	println(server)
	if err != nil {
		log.Fatalf("Failed to initialize Go Proxy: %v\n", err)
	}

	listener, err := net.Listen("tcp", ":8087")
	if err != nil {
		log.Fatalf("Failed to listen on port 8087: %v\n", err)
	}

	// start the server
	grpcServer := grpc.NewServer()
	pb.RegisterGoProxyServer(grpcServer, server)
	log.Println("Go Proxy is running on port 8080")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v\n", err)
	}
}
