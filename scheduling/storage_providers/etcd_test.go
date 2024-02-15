package scheduling

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/joho/godotenv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestStorageProviderETCD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	err := godotenv.Load("../../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	// etcdHost, ok := os.LookupEnv("ETCD_HOST")
	// if !ok {
	// 	t.Fatalf("missing ETCD_HOST variable")
	// }
	// etcdPort, ok := os.LookupEnv("ETCD_PORT")
	// if !ok {
	// 	t.Fatalf("missing ETCD_PORT variable")
	// }
	// etcdUsername, ok := os.LookupEnv("ETCD_USERNAME")
	// if !ok {
	// 	t.Fatalf("missing ETCD_USERNAME variable")
	// }
	// etcdPassword, ok := os.LookupEnv("ETCD_PASSWORD")
	// if !ok {
	// 	t.Fatalf("missing ETCD_PASSWORD variable")
	// }

	etcdHost := "localhost"
	etcdPort := "2379"
	etcdUsername := ""
	etcdPassword := ""

	address := fmt.Sprintf("%s:%s", etcdHost, etcdPort)

	etcdConfig := clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 10,
		Username:    etcdUsername,
		Password:    etcdPassword,
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		panic(err)
	}

	storage := NewETCDStorageProvider(client, context.Background())

	test := StorageProviderTest{
		t:       t,
		storage: storage,
	}
	test.Run()
}
