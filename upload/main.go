package main

import (
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	"os"
)

func main() {
	hdfsConfig := pc.HDFSFileStoreConfig{
		Host:     "localhost",
		Port:     "9000",
		Username: "hduser",
	}

	serializedHDFSConfig, err := hdfsConfig.Serialize()
	if err != nil {
		panic(err)
	}
	hdfsFileStore, err := provider.NewHDFSFileStore(serializedHDFSConfig)
	if err != nil {
		panic(err)
	}
	file, err := os.ReadFile("./ice_cream.parquet")
	if err != nil {
		panic(err)
	}
	err = hdfsFileStore.Write("/ice_cream.parquet", file)
	if err != nil {
		panic(err)
	}

}
