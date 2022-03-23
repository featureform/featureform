package main

import (
	worker "github.com/featureform/serving/runner/worker"
)


func main() {
	if err := worker.CreateAndRun(); err != nil {
		log.Fatalln(err)
	}
}