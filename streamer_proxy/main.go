package main

import (
	"context"
	"io"
	"log"
	"net"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GoProxyServer struct {
	flight.BaseFlightServer
	streamerAddress string
}

func (gps *GoProxyServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	log.Println("Received request: ", string(ticket.GetTicket()))

	// todo: interceptor is set to nil right now, but can include the okta interceptor later
	insecureOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(gps.streamerAddress, nil, nil, insecureOption)
	if err != nil {
		log.Fatalf("Failed to connect to the python streamer: %v", err)
		return err
	}
	defer client.Close()

	// fetch the stream, passing this back to the client caller
	flightStream, err := client.DoGet(context.TODO(), ticket)
	if err != nil {
		log.Printf("Error fetching the data from the python streamer: %v", err)
		return err
	}

	// todo: need to use context cancellation so the caller can set healthy limits
	for {
		flightData, err := flightStream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("Reached the end of the stream")
				break
			}
			// todo: might delete these logs, the err is already getting returned to the proxy caller
			log.Printf("Error reading from the python streamer: %v", err)
			return err
		}

		//send back the flight data, don't need a big buffer, just past back as-is
		sendErr := stream.Send(flightData)
		if sendErr != nil {
			log.Printf("Failed to send data back to client caller: %v/\n", err)
			return err
		}
	}
	return nil
}

func main() {
	//get the gRPC server and stream back to the clients. todo: set the env variables from the container
	serverAddress := "0.0.0.0:8086"
	streamerAddress := "iceberg-streamer:8085"

	flightServer := &GoProxyServer{
		streamerAddress: streamerAddress,
	}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Failed to bind addres to %s: %v", serverAddress, err)
	}

	log.Printf("Starting Go Proxy Flight server on %s...", serverAddress)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)
	servErr := grpcServer.Serve(listener)
	if servErr != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}

}
