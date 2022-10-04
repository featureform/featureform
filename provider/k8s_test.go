//go:build k8s
// +build k8s

package provider

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestBlobInterfaces(t *testing.T) {
	blobTests := map[string]func(*testing.T, BlobStore){
		"Test Blob Read and Write": testBlobReadAndWrite,
		"Test Blob CSV Serve":      testBlobCSVServe,
	}
	localBlobStore, err := NewMemoryBlobStore()
	if err != nil {
		t.Fatalf("Failed to create memory blob store")
	}
	blobProviders := map[string]BlobStore{
		"Local": localBlobStore,
	}
	for testName, blobTest := range blobTests {
		blobTest = blobTest
		testName = testName
		for blobName, blobProvider := range blobProviders {
			blobName = blobName
			blobProvider = blobProvider
			t.Run(fmt.Sprintf("%s: %s", testName, blobName), func(t *testing.T) {
				t.Parallel()
				blobTest(t, blobProvider)
			})
		}
	}
}

func testBlobReadAndWrite(t *testing.T, store BlobStore) {
	testWrite := []byte("example data")
	testKey := uuid.New().String()
	if err := store.Write(testKey, testWrite); err != nil {
		t.Fatalf("Failure writing data %s to key %s: %v", string(testWrite), testKey, err)
	}
	exists, err := store.Exists(testKey)
	if err != nil {
		t.Fatalf("Failure checking existence of key %s: %v", testKey, err)
	}
	if !exists {
		t.Fatalf("Test key %s does not exist: %v", testKey, err)
	}
	readData, err := store.Read(testKey)
	if err != nil {
		t.Fatalf("Could not read key %s from store: %v", testKey, err)
	}
	if string(readData) != string(testWrite) {
		t.Fatalf("Read data does not match written data: %s != %s", readData, testWrite)
	}
}

func testBlobCSVServe(t *testing.T, store BlobStore) {
	//write csv file, then iterate all data types
	csvBytes := []byte(`1,2,3,4,5
	6,7,8,9,10`)
	testKey := fmt.Sprintf("%s.csv", uuid.New().String())
	if err := store.Write(testKey, csvBytes); err != nil {
		t.Fatalf("Failure writing csv data %s to key %s: %v", string(csvBytes), testKey, err)
	}
	iterator, err := store.Serve(testKey)
	if err != nil {
		t.Fatalf("Failure getting serving iterator with key %s: %v", testKey, err)
	}
	for row, err := iterator.Next(); err != nil; row, err = iterator.Next() {
		fmt.Println(row)
	}
	fmt.Println(err)
}
