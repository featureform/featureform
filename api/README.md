# API Server

The API server acts as a gateway to the Featureform cluster. All registration/get and serving 
requests from the [client](../client) are passed through the API server and routed to the [Metadata server](../metadata)
and [Serving server](../newserving) respectively.

It communicates to these services via GRPC using proto definitions located at [/proto](../proto)
and [/metadata/proto](../metadata/proto).

##Files
- *Dockerfile*: The Dockerfile for the API server. Used as a part of [Helm chart](../charts)
- *main.go*: The main file including all GRPC endpoints. 