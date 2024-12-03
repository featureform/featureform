package main

import (
	"context"
	"io"
	"log"
	"net"

	"github.com/apache/arrow/go/v14/arrow/flight"
	pb "github.com/featureform/streamer_proxy/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// gRPC service implementation for Go Proxy
// note: this could change completely depending on a few factors like required inputs, health checks, etc.
type GoProxyServer struct {
	pb.UnimplementedGoProxyServer
	flightClient flight.Client
}

// StreamData is the gRPC method exposed to internal services
func (gps *GoProxyServer) StreamData(req *pb.StreamRequest, stream pb.GoProxy_StreamDataServer) error {
	// Connect to the Python Streamer
	ticket := &flight.Ticket{
		Ticket: []byte(req.TableName),
	}
	flightStream, err := gps.flightClient.DoGet(context.TODO(), ticket)
	if err != nil {
		log.Printf("Failed to fetch data from Python Streamer: %v\n", err)
		return err
	}

	// get the received data stream and pass it through to caller
	for {
		flightData, err := flightStream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("Reached the end")
				break
			}
			log.Printf("End of stream or error: %v\n", err)
			break
		}
		log.Printf("Proxy forward flight data. DataHeader: \n%v \nDataBody \nsize: %d bytes\n",
			flightData.GetDataHeader(), len(flightData.GetDataBody()))

		// forward the record todo: might need to include the flight descriptor
		err = stream.SendMsg(&pb.RecordBatch{
			DataHeader:  flightData.GetDataHeader(),
			DataBody:    flightData.GetDataBody(),
			AppMetadata: flightData.GetAppMetadata(),
		})
		if err != nil {
			log.Printf("Failed to send data back to client caller: %v/\n", err)
			return err
		}
	}

	return nil
}

// NewGoProxyServer initializes the proxy server with a connection to the Python Streamer
func NewGoProxyServer(streamerAddr string) (*GoProxyServer, error) {
	insecureCreds := grpc.WithTransportCredentials(insecure.NewCredentials())
	options := []grpc.DialOption{insecureCreds}
	client, err := flight.NewClientWithMiddleware(streamerAddr, nil, nil, options...)
	if err != nil {
		return nil, err
	}

	return &GoProxyServer{
		flightClient: client,
	}, nil
}

// note: we can alternatively make this a flight server as well, but all clients would then need flight specific libs
// keeping it as a standard gRPC reponse means anyone can call the data.
func main() {
	streamerAddr := "python-streamer:8085"

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
	log.Println("Go Proxy is running on port 8087")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v\n", err)
	}
}
