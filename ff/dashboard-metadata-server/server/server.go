package main

import (
	// "fmt"
	// "net/http"
	"github.com/Sami1309/go-grpc-server/middleware"
	"github.com/Sami1309/go-grpc-server/router"
	//"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	r := router.Router()

	middleware.ConnectGRPC()

	r.Run(":8080")

	
}

