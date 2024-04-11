package provider

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

var (
	cli  *client.Client
	ctx  context.Context
	resp container.CreateResponse
	err  error
)

// To set this flag in the test, at the end of the go test line add -arg -dockerTests=True
// Example:
// go test ./... -v -run TestExample -arg -dockerTests=true
var dockerTests = flag.Bool("dockerTests", false, "Set to true to run and test against local docker containers like localstack and redis.")

// Setup LocalStack Docker container
func setupLocalstack() {
	ctx = context.Background()
	cli, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	// Pull the LocalStack Docker image
	imageName := "localstack/localstack"
	out, err := cli.ImagePull(ctx, imageName, types.ImagePullOptions{})
	if err != nil {
		panic(err)
	}
	out.Close() // Important to close the image pull stream

	// Create the LocalStack container
	resp, err = cli.ContainerCreate(ctx, &container.Config{
		Image: imageName,
		Env:   []string{"SERVICES=s3,lambda,dynamodb"}, // Specify the AWS services you want to mock
		ExposedPorts: nat.PortSet{
			"4566/tcp": struct{}{},
		},
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			"4566/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "4566",
				},
			},
		},
	}, nil, nil, "")
	if err != nil {
		panic(err)
	}

	// Start the LocalStack container
	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		panic(err)
	}

	// These aren't actually checked by localstack
	if err := os.Setenv("DYNAMO_ACCESS_KEY", "test"); err != nil {
		panic(err)
	}
	if err := os.Setenv("DYNAMO_SECRET_KEY", "test"); err != nil {
		panic(err)
	}
	// Has to be http otherwise the client fails since localstack uses a self signed cert.
	if err := os.Setenv("DYNAMO_ENDPOINT", "http://localhost:4566"); err != nil {
		panic(err)
	}
}

func teardownLocalstack() {
	if err := cli.ContainerStop(ctx, resp.ID, container.StopOptions{}); err != nil {
		panic(err)
	}

	// Remove the LocalStack container
	removeOptions := types.ContainerRemoveOptions{
		RemoveVolumes: true, // Remove associated volumes
		Force:         true, // Force removal if necessary
	}
	if err := cli.ContainerRemove(ctx, resp.ID, removeOptions); err != nil {
		panic(err)
	}
}

// TestMain runs setup, calls all tests, and then teardown
func TestMain(m *testing.M) {
	flag.Parse()
	runDocker := *dockerTests && !testing.Short()
	if runDocker {
		defer func() {
			// Teardown even if we panic
			if err := recover(); err != nil {
				teardownLocalstack()
				panic(err)
			}
		}()
		setupLocalstack()
	}
	code := m.Run()
	if runDocker {
		teardownLocalstack()
	}
	os.Exit(code)
}
