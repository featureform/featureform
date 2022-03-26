package main

import (
	worker "github.com/featureform/serving/runner/worker"
	"log"
)

func main() {
	if err := worker.CreateAndRun(); err != nil {
		log.Fatalln(err)
	}
}
