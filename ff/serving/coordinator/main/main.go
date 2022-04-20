package main

import (
	"net"

	"github.com/featureform/serving/coordinator"
	"github.com/featureform/serving/metrics"
	"github.com/featureform/serving/newserving"

	pb "github.com/featureform/serving/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	s, _ := concurrency.NewSession(cli, concurrency.WithTTL(10))
	defer s.Close()

	go func() { //watcher
		for {
			fmt.Println("watching client")
			rch := cli.Watch(context.Background(), "JOB", clientv3.WithPrefix())
			for wresp := range rch {
				for _, ev := range wresp.Events {
					// fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
					// fmt.Println(ev)
					if ev.Type == 0 { //Put type
						go syncHandleJob(string(ev.Kv.Key), s)
					}

				}
			}
		}
	}()

	go startTimedJobWatcher()
}
