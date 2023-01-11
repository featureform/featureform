package main

import (
	"context"
	"encoding/json"
	"fmt"
	help "github.com/featureform/helpers"
	client "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"time"
)

type Data map[string]string

func NewClient(address string) *client.Client {
	cfg := client.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 1,
	}
	c, err := client.New(cfg)
	if err != nil {
		panic(err)
	}
	return c
}

func ReadFile(path string) Data {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	var contents Data
	err = json.Unmarshal(content, &contents)
	if err != nil {
		panic(err)
	}

	return contents
}

func main() {
	host := help.GetEnv("ETCD_HOST", "localhost")
	port := help.GetEnv("ETCD_PORT", "2379")
	address := fmt.Sprintf("%s:%s", host, port)

	fmt.Println("Creating Client")
	c := NewClient(address)
	fmt.Println("Reading File")
	data := ReadFile("./tests/integration/etcd/testcases.json")

	fmt.Println("Inserting Data")
	for k, v := range data {
		fmt.Println("Inserting", k, v)
		_, err := c.Put(context.Background(), k, v)
		if err != nil {
			panic(fmt.Errorf("could not insert k/v pair (%s:%s): %v", k, v, err))
		}
	}
}
