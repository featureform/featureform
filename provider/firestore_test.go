package provider

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func TestOnlineStoreFirestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	secrets := GetSecrets("/testing/firestore")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	project := helpers.GetEnv("FIRESTORE_PROJECT", secrets["FIRESTORE_PROJECT"])

	credentials := helpers.GetEnv("FIRESTORE_CRED", secrets["FIRESTORE_CRED"])

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
