package provider

import (
	"encoding/json"
	"fmt"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"io/ioutil"
	"os"
	"testing"
)

func TestOnlineStoreFirestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	project, ok := os.LookupEnv("FIRESTORE_PROJECT")
	if !ok {
		t.Fatalf("missing FIRESTORE_PROJECT variable")
	}
	credentials, ok := os.LookupEnv("FIRESTORE_CRED")
	if !ok {
		t.Fatalf("missing FIRESTORE_CRED variable")
	}
	JSONCredentials, err := ioutil.ReadFile(credentials)
	if err != nil {
		panic(fmt.Sprintf("Could not open firestore credentials: %v", err))
	}

	var credentialsDict map[string]interface{}
	err = json.Unmarshal(JSONCredentials, &credentialsDict)
	if err != nil {
		panic(fmt.Errorf("cannot unmarshal big query credentials: %v", err))
	}

	firestoreConfig := &pc.FirestoreConfig{
		Collection:  "featureform_test",
		ProjectID:   project,
		Credentials: credentialsDict,
	}

	store, err := GetOnlineStore(pt.FirestoreOnline, firestoreConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
