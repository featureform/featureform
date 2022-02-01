package main

import (
    "go.uber.org/zap"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    pb "github.com/featureform/serving/metadata/proto"
)

type Client struct {
    Logger *zap.SugaredLogger
    conn *grpc.ClientConn
    pb.MetadataClient
}

func NewClient(host string, logger *zap.SugaredLogger) (*Client, error) {
    opts := []grpc.DialOption{
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    }
    conn, err := grpc.Dial(host, opts...)
    if err != nil {
        return nil, err
    }
    defer conn.Close()
    client := pb.NewMetadataClient(conn)
    return &Client{
        Logger: logger,
        conn: conn,
        MetadataClient: client,
    }, nil
}

func (client *Client) Close() {
    client.conn.Close()
}

func main() {
    logger := zap.NewExample().Sugar()
    NewClient("localhost:8080", logger)
}
