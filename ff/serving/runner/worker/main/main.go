package main

import (
	"log"
	worker "github.com/featureform/serving/runner/worker"
)


func main() {
	if err := worker.CreateAndRun(); err != nil {
		log.Fatalln(err)
	}
}