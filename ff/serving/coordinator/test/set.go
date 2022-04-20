package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	provider "github.com/featureform/serving/provider"
	runner "github.com/featureform/serving/runner"
	clientv3 "go.etcd.io/etcd/client/v3"
)


type CoordinatorJob struct {
	Attempts int
	Type metadata.ResourceType
	Name string
	Variant string
}
//change this to the new definition

func worker(wg *sync.WaitGroup, id int, ctx *context.Context, kvc *clientv3.KV) error {
	defer wg.Done()
	jobConfig := CoordinatorJob{
		Attempts: 0,
		Type: "TRAINING_SET_VARIANT",
		Name: "name",
		Variant: "variant",
	}
	serialized, err := jobConfig.Serialize()
	if err != nil {
		return err
	}
	resp, err := (*kvc).Put(*ctx, fmt.Sprintf("JOB_%d", id), string(serialized))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp)
	return nil
}

func main() {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	var wg sync.WaitGroup
	ctx := context.Background()
	fmt.Println("Starting job")
	kvc := clientv3.NewKV(cli)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker(&wg, i, &ctx, &kvc)
	}

	wg.Wait()
	fmt.Println("Main: Completed")

}
