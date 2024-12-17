package main

import (
	"context"
	"io"
	"log"

	"github.com/apache/arrow/go/v17/arrow/flight"
)

type GoProxyServer struct {
	flight.BaseFlightServer
	streamerAddress string
}

func (gps *GoProxyServer) DoGet(ctx context.Context, ticket *flight.Ticket, stream flight.DataStreamWriter) error {
	log.Println("Received request: ", string(ticket.GetTicket()))

	// todo: interceptor is set to nil right now, but can include the okta interceptor later
	client, err := flight.NewClientWithMiddleware(gps.streamerAddress, nil, nil)
	if err != nil {
		log.Fatalf("Failed to connect to the python streamer: %v", err)
		return err
	}
	defer client.Close()

	// fetch the stream, passing this back to the client caller
	flightStream, err := client.DoGet(ctx, ticket)
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
