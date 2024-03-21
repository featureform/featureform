package scheduling

import (
	"testing"
)

func TestStorageProviderETCD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	// etcdHost := "localhost"
	// etcdPort := "2379"
	// etcdUsername := ""
	// etcdPassword := ""

	// address := fmt.Sprintf("%s:%s", etcdHost, etcdPort)

	// etcdConfig := clientv3.Config{
	// 	Endpoints:   []string{address},
	// 	DialTimeout: time.Second * 10,
	// 	Username:    etcdUsername,
	// 	Password:    etcdPassword,
	// }

	// client, err := clientv3.New(etcdConfig)
	// if err != nil {
	// 	t.Fatalf("Error creating etcd client: %v", err)
	// }

	// storage := NewETCDStorageProvider(client, context.Background())

	// test := StorageProviderTest{
	// 	t:       t,
	// 	storage: storage,
	// }
	// test.Run()
}
