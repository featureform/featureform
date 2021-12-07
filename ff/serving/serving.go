package main

import (
	"log"
	"net"

	pb "github.com/featureform/serving/proto"
	"google.golang.org/grpc"
)

type TrainingDataServer struct {
	pb.UnimplementedOfflineServingServer
}

func (serv *TrainingDataServer) TrainingData(req *pb.TrainingDataRequest, stream pb.OfflineServing_TrainingDataServer) error {
    val := &pb.Value{
        Value: &pb.Value_StrValue{"xyz"},
    }
    label := &pb.Value{
        Value: &pb.Value_FloatValue{12.32},
    }
    row := &pb.TrainingDataRow{
        Features: []*pb.Value{val},
        Label: label,
    }
    if err := stream.Send(row); err != nil {
        return err
    }
    return nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("%s", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterOfflineServingServer(grpcServer, &TrainingDataServer{})
	grpcServer.Serve(lis)
}
