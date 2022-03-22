package runner

import (
	"errors"
	"log"
	"os"
)

func createAndRun() error {
	config, ok := os.LookupEnv("CONFIG")
	if !ok {
		return errors.New("CONFIG not set")
	}
	name, ok := os.LookupEnv("NAME")
	if !ok {
		return errors.New("NAME not set")
	}
	runner, err := Create(name, []byte(config))
	if err != nil {
		return err
	}
	watcher, err := runner.Run()
	if err != nil {
		return err
	}
	if err := watcher.Wait(); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := createAndRun(); err != nil {
		log.Fatalln(err)
	}
}
