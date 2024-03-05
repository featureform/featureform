package provider

import (
	"os"
	"testing"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

// TODO!

func TestOnlineStoreIKV(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	storeName, ok := os.LookupEnv("IKV_STORE_NAME")
	if !ok {
		t.Fatalf("missing IKV_STORE_NAME variable")
	}

	accountId, ok := os.LookupEnv("IKV_ACCOUNT_ID")
	if !ok {
		t.Fatalf("missing IKV_STORE_NAME variable")
	}

	accountPasskey, ok := os.LookupEnv("IKV_ACCOUNT_PASSKEY")
	if !ok {
		t.Fatalf("missing IKV_STORE_NAME variable")
	}

	mountDir, ok := os.LookupEnv("IKV_MOUNT_DIRECTORY")
	if !ok {
		t.Fatalf("missing IKV_STORE_NAME variable")
	}

	logLevel, ok := os.LookupEnv("IKV_LOG_LEVEL")
	if !ok {
		logLevel = "info"
	}

	logFilePath, ok := os.LookupEnv("IKV_LOG_FILE_PATH")
	if !ok {
		logFilePath = ""
	}

	ikvConfig := &pc.IKVConfig{
		StoreName:      storeName,
		AccountId:      accountId,
		AccountPasskey: accountPasskey,
		MountDirectory: mountDir,
		LogLevel:       logLevel,
		LogFilePath:    logFilePath,
	}

	store, err := GetOnlineStore(pt.IKVOnline, ikvConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
