package middleware

import (
	//remote packages
	"context"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	pb "github.com/Sami1309/go-grpc-server/grpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type metadata struct {
	VersionName string   `json:"version-name"`
	Dimensions  int32    `json:"dimensions"`
	Created     string   `json:"created"`
	Owner       string   `json:"owner"`
	Visibility  string   `json:"visibility"`
	Revision    string   `json:"revision"`
	Tags        []string `json:"tags"`
	Description string   `json:"description"`
}

type data struct {
}

type version struct {
	Metadata metadata `json:"metadata"`
	Data     data     `json:"data"`
}

type space struct {
	Name           string             `json:"name"`
	DefaultVersion string             `json:"default-version"`
	Type           string             `json:"type"`
	AllVersions    []string           `json:"all-versions"`
	Versions       map[string]version `json:"versions"`
}

type listed_space struct {
	Name           string              `json:"name"`
	DefaultVersion string              `json:"default-version"`
	Type           string              `json:"type"`
	AllVersions    []string            `json:"all-versions"`
	Versions       map[string]metadata `json:"versions"`
}

var getEmbeddingCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "http_get_embeddings", // metric name
		Help: "Number of embeddings get requests.",
	},
	[]string{"space","key","status"}, // labels
)

var getEmbeddingLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "http_get_embeddings_duration_seconds",
		Help:    "Latency of embeddings get requests.",
		Buckets: prometheus.LinearBuckets(0.01, 0.05, 10),
	},
	[]string{"space","key","status"}, //labels
)

func init() {
	prometheus.MustRegister(getEmbeddingCounter)
	prometheus.MustRegister(getEmbeddingLatency)
}

func PrometheusHandler() gin.HandlerFunc {
    h := promhttp.Handler()

    return func(c *gin.Context) {
        h.ServeHTTP(c.Writer, c.Request)
    }
}




//Retrieve list of all avaliable data types
// router.GET("/list", middleware.GetTypes)
//RETURNS json list of named types
func GetTypes(c *gin.Context) {
	var type_name string
	var name string
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		c.JSON(500, gin.H{"Error": "Could not connect to grpc"})
		return
	}

	client := pb.NewEmbeddingHubClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	type_name = c.Param("type")
	name = c.Param("name")

	type_struct, err := GetTypeStruct(type)
	if err != nil {
		c.JSON(400, gin.H{"Error:": "Invalid Type"})
	}

	getResponse, getResponseErr := client.Get(ctx, &pb.GetRequest{Type: type, Name: name})
	if getResponseErr != nil {
		c.JSON(500, gin.H{"Error": "Problem fetching object metadata"})
		return
	}

	type_json, err := ConvertTypeStruct(type_name, getResponse.GetMetadata())
	if err != nil {
		c.JSON(500, gin.H{"Error": "Problem with metadata structure"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"Metadata": getResponse.GetMetadata()})
}

//Retrieve list of all of a specific data type (feature, label, training set, etc)
// router.GET("/:type", middleware.GetType)
//RETURNS json list of data by given type
func GetType(c *gin.Context) {
	var type_name string
	var name string
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		c.JSON(500, gin.H{"Error": "Could not connect to grpc"})
		return
	}

	client := pb.NewEmbeddingHubClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	type_name = c.Param("type")
	name = c.Param("name")

	type_struct, err := GetTypeStruct(type)
	if err != nil {
		c.JSON(400, gin.H{"Error:": "Invalid Type"})
	}

	getResponse, getResponseErr := client.Get(ctx, &pb.GetRequest{Type: type, Name: name})
	if getResponseErr != nil {
		c.JSON(500, gin.H{"Error": "Problem fetching object metadata"})
		return
	}

	type_json, err := ConvertTypeStruct(type_name, getResponse.GetMetadata())
	if err != nil {
		c.JSON(500, gin.H{"Error": "Problem with metadata structure"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"Metadata": getResponse.GetMetadata()})
}

//Retrieve all versions of metadata for specific object
// router.GET("/:type/:name", middleware.GetObject)
func GetObject(c *gin.Context) {
	var type_name string
	var name string
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		c.JSON(500, gin.H{"Error": "Could not connect to grpc"})
		return
	}

	client := pb.NewEmbeddingHubClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	type_name = c.Param("type")
	name = c.Param("name")

	type_struct, err := GetTypeStruct(type)
	if err != nil {
		c.JSON(400, gin.H{"Error:": "Invalid Type"})
	}

	getResponse, getResponseErr := client.Get(ctx, &pb.GetRequest{Type: type, Name: name})
	if getResponseErr != nil {
		c.JSON(500, gin.H{"Error": "Problem fetching object metadata"})
		return
	}

	type_json, err := ConvertTypeStruct(type_name, getResponse.GetMetadata())
	if err != nil {
		c.JSON(500, gin.H{"Error": "Problem with metadata structure"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"Metadata": getResponse.GetMetadata()})
}

//API: router.GET("/spaces/:name/:key", middleware.GetEmbedding)
func GetEmbedding(c *gin.Context) {

	var key string
	var name string
	status := "error"
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		getEmbeddingLatency.WithLabelValues(name,key,status).Observe(v)
	}))
	defer func() {
		//record count and latency metrics upon function exit
		getEmbeddingCounter.WithLabelValues(name,key,status).Inc()
		timer.ObserveDuration()
	}()
	
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		c.JSON(500, gin.H{"Error": "Could not connect to grpc"})
		return
	}

	client := pb.NewEmbeddingHubClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	name = c.Param("name")
	key = c.Param("key")
	getResponse, getResponseErr := client.Get(ctx, &pb.GetRequest{Key: key, Space: name})
	if getResponseErr != nil {
		c.JSON(500, gin.H{"Error": "Problem fetching embedding"})
		return
	}


	log.Printf("Retrieved vector: %s", getResponse.GetEmbedding())
	if getResponse.GetEmbedding().GetValues() == nil {
		c.JSON(404, gin.H{"Error": "Problem fetching embedding"})
		return
	}
	status = "success"
	c.JSON(http.StatusOK, gin.H{"Space": getResponse.GetEmbedding()})

}

//API: router.GET("/spaces/:name", middleware.GetSpaceVectors)
func GetEmbeddings(c *gin.Context) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	CLIENT := pb.NewEmbeddingHubClient(conn)

	name := c.Param("name")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, downloadError := CLIENT.Download(ctx, &pb.DownloadRequest{Space: name})
	if downloadError != nil {
		log.Fatalf("Error message: ( %v)", downloadError)
	}

	var embedding_list []string

	done := make(chan bool)

	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				done <- true //means stream is finished
				return
			}
			if err != nil {
				log.Fatalf("cannot receive %v", err)
			}
			embedding_list = append(embedding_list, resp.String())
			log.Printf("Embedding received: %s", resp.String())
		}
	}()

	<-done //we will wait until all response is received
	log.Printf("finished")
	c.JSON(http.StatusOK, gin.H{"Embeddings": embedding_list})
}

