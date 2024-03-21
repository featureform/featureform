package metadata

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/featureform/metadata/proto"
	"golang.org/x/exp/slices"
	grpc_status "google.golang.org/grpc/status"
	tspb "google.golang.org/protobuf/types/known/timestamppb"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"go.uber.org/zap/zaptest"
)

const PythonFunc = `def average_user_transaction(transactions):
	return transactions.groupby("CustomerID")["TransactionAmount"].mean()`

func TestResourceTypes(t *testing.T) {
	typeMapping := map[ResourceType]ResourceDef{
		USER:                 UserDef{},
		PROVIDER:             ProviderDef{},
		ENTITY:               EntityDef{},
		SOURCE_VARIANT:       SourceDef{},
		FEATURE_VARIANT:      FeatureDef{},
		LABEL_VARIANT:        LabelDef{},
		TRAINING_SET_VARIANT: TrainingSetDef{},
		MODEL:                ModelDef{},
		TRIGGER:              TriggerDef{},
	}
	for typ, def := range typeMapping {
		if def.ResourceType() != typ {
			t.Fatalf("Expected %T ResourceType to be %s found %s", def, typ, def.ResourceType())
		}
	}
}

func filledResourceDefs() []ResourceDef {
	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "featureformer",
		Password:     "password",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "sysadmin",
	}
	return []ResourceDef{
		UserDef{
			Name:       "Featureform",
			Tags:       Tags{},
			Properties: Properties{},
		},
		UserDef{
			Name:       "Other",
			Tags:       Tags{},
			Properties: Properties{},
		},
		TriggerDef{
			Name:            "trigger1",
			ScheduleTrigger: "* * * * *",
		},
		TriggerDef{
			Name:            "trigger2",
			ScheduleTrigger: "1 2 * * *",
		},
		TriggerDef{
			Name:            "trigger3",
			ScheduleTrigger: "2 3 * * *",
		},
		ProviderDef{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: redisConfig.Serialized(),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		ProviderDef{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: snowflakeConfig.Serialize(),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		EntityDef{
			Name:        "user",
			Description: "A user entity",
			Tags:        Tags{},
			Properties:  Properties{},
		},
		EntityDef{
			Name:        "item",
			Description: "An item entity",
			Tags:        Tags{},
			Properties:  Properties{},
		},
		SourceDef{
			Name:        "mockSource",
			Variant:     "var",
			Description: "A CSV source",
			Definition: TransformationSource{
				TransformationType: SQLTransformationType{
					Query: "SELECT * FROM dummy",
					Sources: []NameVariant{{
						Name:    "mockName",
						Variant: "mockVariant"},
					},
				},
			},
			Owner:      "Featureform",
			Provider:   "mockOffline",
			Tags:       Tags{},
			Properties: Properties{},
			TaskIDs:    []int32{1},
			JobID:      2,
			Triggers:   []string{"trigger2"},
		},
		SourceDef{
			Name:        "mockSource",
			Variant:     "var2",
			Description: "A CSV source but different",
			Definition: PrimaryDataSource{
				Location: SQLTable{
					Name: "mockPrimary",
				},
			},
			Owner:      "Featureform",
			Provider:   "mockOffline",
			Tags:       Tags{},
			Properties: Properties{},
			TaskIDs:    []int32{3},
			JobID:      4,
			Triggers:   []string{"trigger1"},
		},
		FeatureDef{
			Name:        "feature",
			Variant:     "variant",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        "float",
			Description: "Feature variant",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Featureform",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       PRECOMPUTED,
			IsOnDemand: false,
			TaskIDs:    []int32{5},
			JobID:      6,
			Triggers:   []string{"trigger2"},
		},
		FeatureDef{
			Name:        "feature",
			Variant:     "variant2",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        "int",
			Description: "Feature variant2",
			Source:      NameVariant{"mockSource", "var2"},
			Owner:       "Featureform",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       PRECOMPUTED,
			IsOnDemand: false,
			TaskIDs:    []int32{7},
			JobID:      8,
			Triggers:   []string{"trigger3"},
		},
		FeatureDef{
			Name:        "feature2",
			Variant:     "variant",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        "string",
			Description: "Feature2 variant",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Featureform",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       PRECOMPUTED,
			IsOnDemand: false,
			TaskIDs:    []int32{9},
			JobID:      10,
			Triggers:   []string{"trigger3"},
		},
		FeatureDef{
			Name:        "feature3",
			Variant:     "on-demand",
			Description: "Feature3 on-demand",
			Owner:       "Featureform",
			Location: PythonFunction{
				Query: []byte(PythonFunc),
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       CLIENT_COMPUTED,
			IsOnDemand: true,
			TaskIDs:    []int32{11},
			JobID:      12,
			Triggers:   []string{"trigger2"},
		},
		LabelDef{
			Name:        "label",
			Variant:     "variant",
			Type:        "int64",
			Description: "label variant",
			Provider:    "mockOffline",
			Entity:      "user",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Other",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
			TaskIDs:    []int32{13},
			JobID:      14,
			Triggers:   []string{"trigger1"},
		},
		TrainingSetDef{
			Name:        "training-set",
			Variant:     "variant",
			Provider:    "mockOffline",
			Description: "training-set variant",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature", "variant"},
				{"feature", "variant2"},
			},
			Owner:      "Other",
			Tags:       Tags{},
			Properties: Properties{},
			TaskIDs:    []int32{15},
			JobID:      16,
			Triggers:   []string{"trigger1"},
		},
		TrainingSetDef{
			Name:        "training-set",
			Variant:     "variant2",
			Provider:    "mockOffline",
			Description: "training-set variant2",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			Owner:      "Featureform",
			Tags:       Tags{},
			Properties: Properties{},
			TaskIDs:    []int32{17},
			JobID:      18,
			Triggers:   []string{"trigger3"},
		},
		ModelDef{
			Name:         "fraud",
			Description:  "fraud model",
			Features:     NameVariants{},
			Trainingsets: NameVariants{},
			Tags:         Tags{},
			Properties:   Properties{},
		},
	}
}

func list(client *Client, t ResourceType) (interface{}, error) {
	ctx := context.Background()
	switch t {
	case FEATURE:
		return client.ListFeatures(ctx)
	case LABEL:
		return client.ListLabels(ctx)
	case SOURCE:
		return client.ListSources(ctx)
	case TRAINING_SET:
		return client.ListTrainingSets(ctx)
	case USER:
		return client.ListUsers(ctx)
	case ENTITY:
		return client.ListEntities(ctx)
	case MODEL:
		return client.ListModels(ctx)
	case PROVIDER:
		return client.ListProviders(ctx)
	case TRIGGER:
		return client.ListTriggers(ctx)
	default:
		panic("ResourceType not handled")
	}
}

func getAll(client *Client, t ResourceType, nameVars NameVariants) (interface{}, error) {
	ctx := context.Background()
	switch t {
	case FEATURE:
		return client.GetFeatures(ctx, nameVars.Names())
	case FEATURE_VARIANT:
		return client.GetFeatureVariants(ctx, nameVars)
	case LABEL:
		return client.GetLabels(ctx, nameVars.Names())
	case LABEL_VARIANT:
		return client.GetLabelVariants(ctx, nameVars)
	case SOURCE:
		return client.GetSources(ctx, nameVars.Names())
	case SOURCE_VARIANT:
		return client.GetSourceVariants(ctx, nameVars)
	case TRAINING_SET:
		return client.GetTrainingSets(ctx, nameVars.Names())
	case TRAINING_SET_VARIANT:
		return client.GetTrainingSetVariants(ctx, nameVars)
	case USER:
		return client.GetUsers(ctx, nameVars.Names())
	case ENTITY:
		return client.GetEntities(ctx, nameVars.Names())
	case MODEL:
		return client.GetModels(ctx, nameVars.Names())
	case PROVIDER:
		return client.GetProviders(ctx, nameVars.Names())
	case TRIGGER:
		return client.GetTriggers(ctx, nameVars.Names())
	default:
		panic("ResourceType not handled")
	}
}

func get(client *Client, t ResourceType, nameVar NameVariant) (interface{}, error) {
	ctx := context.Background()
	switch t {
	case FEATURE:
		return client.GetFeature(ctx, nameVar.Name)
	case FEATURE_VARIANT:
		return client.GetFeatureVariant(ctx, nameVar)
	case LABEL:
		return client.GetLabel(ctx, nameVar.Name)
	case LABEL_VARIANT:
		return client.GetLabelVariant(ctx, nameVar)
	case SOURCE:
		return client.GetSource(ctx, nameVar.Name)
	case SOURCE_VARIANT:
		return client.GetSourceVariant(ctx, nameVar)
	case TRAINING_SET:
		return client.GetTrainingSet(ctx, nameVar.Name)
	case TRAINING_SET_VARIANT:
		return client.GetTrainingSetVariant(ctx, nameVar)
	case USER:
		return client.GetUser(ctx, nameVar.Name)
	case ENTITY:
		return client.GetEntity(ctx, nameVar.Name)
	case MODEL:
		return client.GetModel(ctx, nameVar.Name)
	case PROVIDER:
		return client.GetProvider(ctx, nameVar.Name)
	case TRIGGER:
		return client.GetTrigger(ctx, nameVar.Name)
	default:
		panic("ResourceType not handled")
	}
}

func update(client *Client, t ResourceType, def ResourceDef) error {
	ctx := context.Background()
	switch t {
	case FEATURE_VARIANT:
		casted := def.(FeatureDef)
		return client.CreateFeatureVariant(ctx, casted)
	case LABEL_VARIANT:
		casted := def.(LabelDef)
		return client.CreateLabelVariant(ctx, casted)
	case SOURCE_VARIANT:
		casted := def.(SourceDef)
		return client.CreateSourceVariant(ctx, casted)
	case TRAINING_SET_VARIANT:
		casted := def.(TrainingSetDef)
		return client.CreateTrainingSetVariant(ctx, casted)
	case USER:
		casted := def.(UserDef)
		return client.CreateUser(ctx, casted)
	case ENTITY:
		casted := def.(EntityDef)
		return client.CreateEntity(ctx, casted)
	case MODEL:
		casted := def.(ModelDef)
		return client.CreateModel(ctx, casted)
	case PROVIDER:
		casted := def.(ProviderDef)
		return client.CreateProvider(ctx, casted)
	case TRIGGER:
		casted := def.(TriggerDef)
		return client.CreateTrigger(ctx, casted)
	default:
		panic("ResourceType not handled")
	}
}

type testContext struct {
	Defs   []ResourceDef
	serv   *MetadataServer
	client *Client
}

func (ctx *testContext) Create(t *testing.T) (*Client, error) {
	var addr string
	ctx.serv, addr = startServ(t)
	ctx.client = client(t, addr)
	if err := ctx.client.CreateAll(context.Background(), ctx.Defs); err != nil {
		return nil, err
	}
	return ctx.client, nil
}

func (ctx *testContext) Destroy() {
	ctx.serv.Stop()
	ctx.client.Close()
}

func startServ(t *testing.T) (*MetadataServer, string) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Logger:          logger.Sugar(),
		StorageProvider: LocalStorageProvider{},
	}
	serv, err := NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return serv, lis.Addr().String()
}

func startServNoPanic(t *testing.T) (*MetadataServer, string) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Logger:          logger.Sugar(),
		StorageProvider: LocalStorageProvider{},
	}
	serv, err := NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			t.Logf("Server error: %s", err)
		}
	}()
	return serv, lis.Addr().String()
}

func client(t *testing.T, addr string) *Client {
	logger := zaptest.NewLogger(t).Sugar()
	client, err := NewClient(addr, logger)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	return client
}

func TestClosedServer(t *testing.T) {
	serv, addr := startServNoPanic(t)
	client := client(t, addr)
	for {
		if serv.Stop() == nil {
			break
		}
	}
	listTypes := []ResourceType{
		FEATURE,
		LABEL,
		SOURCE,
		TRAINING_SET,
		USER,
		ENTITY,
		MODEL,
		PROVIDER,
	}
	for _, typ := range listTypes {
		if _, err := list(client, typ); err == nil {
			t.Fatalf("Succeeded in listing from closed server")
		}
	}
	types := []ResourceType{
		FEATURE,
		FEATURE_VARIANT,
		LABEL,
		LABEL_VARIANT,
		SOURCE,
		SOURCE_VARIANT,
		TRAINING_SET,
		TRAINING_SET_VARIANT,
		USER,
		ENTITY,
		MODEL,
		PROVIDER,
	}
	for _, typ := range types {
		if _, err := getAll(client, typ, []NameVariant{}); err == nil {
			t.Fatalf("Succeeded in getting all from closed server")
		}
		if _, err := get(client, typ, NameVariant{}); err == nil {
			t.Fatalf("Succeeded in getting from closed server")
		}
	}
}

func TestServeGracefulStop(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		Logger:          logger.Sugar(),
		StorageProvider: LocalStorageProvider{},
		Address:         ":0",
	}
	serv, err := NewMetadataServer(config)
	if err != nil {
		t.Fatalf("Failed to create metadata server: %s", err)
	}
	errChan := make(chan error)
	go func() {
		errChan <- serv.Serve()
	}()
	for {
		if err := serv.GracefulStop(); err == nil {
			break
		}
	}
	select {
	case <-errChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("GracefulStop did not work")
	}
}

func TestCreate(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	_, err := ctx.Create(t)
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}
	defer ctx.Destroy()
}

func assertEqual(t *testing.T, this, that interface{}) {
	t.Helper()
	if !reflect.DeepEqual(this, that) {
		t.Fatalf("Values not equal\nActual: %#v\nExpected: %#v", this, that)
	}
}

func assertEquivalentNameVariants(t *testing.T, this, that []NameVariant) {
	t.Helper()
	if len(this) != len(that) {
		t.Fatalf("NameVariants not equal\n%+v\n%+v", this, that)
	}
	thisMap := make(map[NameVariant]bool)
	for _, val := range this {
		thisMap[val] = true
	}
	for _, val := range that {
		if _, has := thisMap[val]; !has {
			t.Fatalf("NameVariants not equal, value not found %+v\n%+v\n%+v", val, this, that)
		}
	}
}

func assertEquivalentStrings(t *testing.T, this, that []string) {
	t.Helper()
	if len(this) != len(that) {
		t.Fatalf("Lists not equal\n%+v\n%+v", this, that)
	}
	thisMap := make(map[string]bool)
	for _, val := range this {
		thisMap[val] = true
	}
	for _, val := range that {
		if _, has := thisMap[val]; !has {
			t.Fatalf("Lists not equal, value not found %+v\n%+v\n%+v", val, this, that)
		}
	}
}

func assertEquivalentInt(t *testing.T, this, that []int32) {
	t.Helper()
	if len(this) != len(that) {
		t.Fatalf("Lists not equal\n%+v\n%+v", this, that)
	}
	thisMap := make(map[int32]bool)
	for _, val := range this {
		thisMap[val] = true
	}
	for _, val := range that {
		if _, has := thisMap[val]; !has {
			t.Fatalf("Lists not equal, value not found %+v\n%+v\n%+v", val, this, that)
		}
	}
}

type UserTest struct {
	Name         string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
	Sources      []NameVariant
	Tags         Tags
	Properties   Properties
}

func (test UserTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test UserTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	user := res.(*User)
	assertEqual(t, user.Name(), test.Name)
	assertEquivalentNameVariants(t, user.Features(), test.Features)
	assertEquivalentNameVariants(t, user.Labels(), test.Labels)
	assertEquivalentNameVariants(t, user.TrainingSets(), test.TrainingSets)
	assertEquivalentNameVariants(t, user.Sources(), test.Sources)
	if shouldFetch {
		testFetchTrainingSets(t, client, user)
		testFetchLabels(t, client, user)
		testFetchFeatures(t, client, user)
		testFetchSources(t, client, user)
	}
}

func expectedUsers() ResourceTests {
	return ResourceTests{
		UserTest{
			Name:   "Featureform",
			Labels: []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
				{"feature3", "on-demand"},
			},
			Sources: []NameVariant{
				{"mockSource", "var"},
				{"mockSource", "var2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant2"},
			},
		},
		UserTest{
			Name: "Other",
			Labels: []NameVariant{
				{"label", "variant"},
			},
			Features: []NameVariant{},
			Sources:  []NameVariant{},
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
			},
		},
	}
}

func userUpdates() []ResourceDef {
	return []ResourceDef{
		UserDef{
			Name:       "Featureform",
			Tags:       Tags{"primary_user"},
			Properties: Properties{"usr_key_1": "usr_val_1"},
		},
		UserDef{
			Name:       "Featureform",
			Tags:       Tags{"active"},
			Properties: Properties{"usr_key_1": "user_value_1"},
		},
	}
}

func expectedUserUpdates() ResourceTests {
	return ResourceTests{
		UserTest{
			Name:   "Featureform",
			Labels: []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
				{"feature3", "on-demand"},
			},
			Sources: []NameVariant{
				{"mockSource", "var"},
				{"mockSource", "var2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant2"},
			},
			Tags:       Tags{"primary_user"},
			Properties: Properties{"usr_key_1": "usr_val_1"},
		},
		UserTest{
			Name:   "Featureform",
			Labels: []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
				{"feature3", "on-demand"},
			},
			Sources: []NameVariant{
				{"mockSource", "var"},
				{"mockSource", "var2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant2"},
			},
			Tags:       Tags{"primary_user", "active"},
			Properties: Properties{"usr_key_1": "user_value_1"},
		},
	}
}

func TestUser(t *testing.T) {
	testListResources(t, USER, expectedUsers())
	testGetResources(t, USER, expectedUsers())
	testResourceUpdates(t, USER, expectedUsers(), expectedUserUpdates(), userUpdates())
}

type ProviderTest struct {
	Name             string
	Description      string
	Team             string
	Type             string
	Software         string
	SerializedConfig []byte
	Features         []NameVariant
	Labels           []NameVariant
	TrainingSets     []NameVariant
	Sources          []NameVariant
	Tags             Tags
	Properties       Properties
}

func (test ProviderTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test ProviderTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	provider := res.(*Provider)
	assertEqual(t, provider.Name(), test.Name)
	assertEqual(t, provider.Team(), test.Team)
	assertEqual(t, provider.Type(), test.Type)
	assertEqual(t, provider.Description(), test.Description)
	assertEqual(t, provider.Software(), test.Software)
	assertEqual(t, provider.SerializedConfig(), test.SerializedConfig)
	assertEqual(t, provider.Tags(), test.Tags)
	assertEqual(t, provider.Properties(), test.Properties)
	assertEquivalentNameVariants(t, provider.Features(), test.Features)
	assertEquivalentNameVariants(t, provider.Labels(), test.Labels)
	assertEquivalentNameVariants(t, provider.TrainingSets(), test.TrainingSets)
	assertEquivalentNameVariants(t, provider.Sources(), test.Sources)
	if shouldFetch {
		testFetchFeatures(t, client, provider)
		testFetchLabels(t, client, provider)
		testFetchTrainingSets(t, client, provider)
		testFetchSources(t, client, provider)
	}
}

func expectedProviders() ResourceTests {
	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "featureformer",
		Password:     "password",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "sysadmin",
	}
	return ResourceTests{
		ProviderTest{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: redisConfig.Serialized(),
			Labels:           []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			Sources:      []NameVariant{},
			TrainingSets: []NameVariant{},
			Tags:         Tags{},
			Properties:   Properties{},
		},
		ProviderTest{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: snowflakeConfig.Serialize(),
			Labels: []NameVariant{
				{"label", "variant"},
			},
			Features: []NameVariant{},
			Sources: []NameVariant{
				{"mockSource", "var"},
				{"mockSource", "var2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
			Tags:       Tags{},
			Properties: Properties{},
		},
	}
}

func providerUpdates() []ResourceDef {
	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root123",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "feature-former",
		Password:     "password123",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "ff-user",
	}
	return []ResourceDef{
		ProviderDef{
			Name:             "mockOnline",
			Description:      "An updated mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: redisConfig.Serialized(),
			Tags:             Tags{"online"},
		},
		ProviderDef{
			Name:             "mockOffline",
			Description:      "An updated mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: snowflakeConfig.Serialize(),
			Tags:             Tags{"offline"},
		},
	}
}

func expectedUpdatedProviders() ResourceTests {
	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root123",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "feature-former",
		Password:     "password123",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "ff-user",
	}
	return ResourceTests{
		ProviderTest{
			Name:             "mockOnline",
			Description:      "An updated mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: redisConfig.Serialized(),
			Labels:           []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			Sources:      []NameVariant{},
			TrainingSets: []NameVariant{},
			Tags:         Tags{"online"},
			Properties:   Properties{},
		},
		ProviderTest{
			Name:             "mockOffline",
			Description:      "An updated mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: snowflakeConfig.Serialize(),
			Labels: []NameVariant{
				{"label", "variant"},
			},
			Features: []NameVariant{},
			Sources: []NameVariant{
				{"mockSource", "var"},
				{"mockSource", "var2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
			Tags:       Tags{"offline"},
			Properties: Properties{},
		},
	}
}

func TestProvider(t *testing.T) {
	testListResources(t, PROVIDER, expectedProviders())
	testGetResources(t, PROVIDER, expectedProviders())
	testResourceUpdates(t, PROVIDER, expectedProviders(), expectedUpdatedProviders(), providerUpdates())
}

type EntityTest struct {
	Name         string
	Description  string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
	Sources      []NameVariant
}

func (test EntityTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test EntityTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	t.Logf("Testing entity: %s", test.Name)
	entity := res.(*Entity)
	assertEqual(t, entity.Name(), test.Name)
	assertEqual(t, entity.Description(), test.Description)
	assertEquivalentNameVariants(t, entity.Features(), test.Features)
	assertEquivalentNameVariants(t, entity.Labels(), test.Labels)
	assertEquivalentNameVariants(t, entity.TrainingSets(), test.TrainingSets)
	if shouldFetch {
		testFetchLabels(t, client, entity)
		testFetchFeatures(t, client, entity)
		testFetchTrainingSets(t, client, entity)
	}
}

func expectedEntities() ResourceTests {
	return ResourceTests{
		EntityTest{
			Name:        "user",
			Description: "A user entity",
			Labels: []NameVariant{
				{"label", "variant"},
			},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
		},
		EntityTest{
			Name:         "item",
			Description:  "An item entity",
			Labels:       []NameVariant{},
			Features:     []NameVariant{},
			TrainingSets: []NameVariant{},
		},
	}
}

func TestEntity(t *testing.T) {
	testListResources(t, ENTITY, expectedEntities())
	testGetResources(t, ENTITY, expectedEntities())
}

type SourceVariantTest struct {
	Name                     string
	Variant                  string
	Description              string
	Owner                    string
	Provider                 string
	Features                 []NameVariant
	Labels                   []NameVariant
	TrainingSets             []NameVariant
	IsTransformation         bool
	IsSQLTransformation      bool
	IsPrimaryData            bool
	IsPrimaryDataSQLTable    bool
	PrimaryDataSQLTableName  string
	SQLTransformationSources []NameVariant
	TaskIDs                  []int32
	JobID                    int32
	Triggers                 []string
}

func (test SourceVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test SourceVariantTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	t.Logf("Testing source: %s %s", test.Name, test.Variant)
	source := res.(*SourceVariant)
	assertEqual(t, source.Name(), test.Name)
	assertEqual(t, source.Variant(), test.Variant)
	assertEqual(t, source.Description(), test.Description)
	assertEqual(t, source.Owner(), test.Owner)
	assertEqual(t, source.Provider(), test.Provider)
	assertEquivalentNameVariants(t, source.Features(), test.Features)
	assertEquivalentNameVariants(t, source.Labels(), test.Labels)
	assertEquivalentNameVariants(t, source.TrainingSets(), test.TrainingSets)
	assertEqual(t, source.IsTransformation(), test.IsTransformation)
	assertEqual(t, source.IsSQLTransformation(), test.IsSQLTransformation)
	assertEqual(t, source.SQLTransformationSources(), test.SQLTransformationSources)
	assertEqual(t, source.isPrimaryData(), test.IsPrimaryData)
	assertEqual(t, source.IsPrimaryDataSQLTable(), test.IsPrimaryDataSQLTable)
	assertEqual(t, source.PrimaryDataSQLTableName(), test.PrimaryDataSQLTableName)
	if shouldFetch {
		testFetchProvider(t, client, source)
		testFetchFeatures(t, client, source)
		testFetchLabels(t, client, source)
		testFetchTrainingSets(t, client, source)
	}
}

type TriggerTest struct {
	Name            string
	ScheduleTrigger string
	TaskIDs         []int32
	JobIDs          []int32
}

func (test TriggerTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test TriggerTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	trigger := res.(*Trigger)
	assertEqual(t, trigger.Name(), test.Name)
	assertEqual(t, trigger.Schedule(), test.ScheduleTrigger)
	assertEquivalentInt(t, trigger.TaskIDs(), test.TaskIDs)
	assertEquivalentInt(t, trigger.JobIDs(), test.JobIDs)

}

func expectedTrigger() ResourceTests {
	return ResourceTests{
		TriggerTest{
			Name:            "trigger1",
			ScheduleTrigger: "* * * * *",
			TaskIDs:         []int32{3, 13, 15},
			JobIDs:          []int32{4, 14, 16},
		},
		TriggerTest{
			Name:            "trigger2",
			ScheduleTrigger: "1 2 * * *",
			TaskIDs:         []int32{1, 5, 11},
			JobIDs:          []int32{2, 6, 12},
		},
		TriggerTest{
			Name:            "trigger3",
			ScheduleTrigger: "2 3 * * *",
			TaskIDs:         []int32{7, 9, 17},
			JobIDs:          []int32{8, 10, 18},
		},
	}
}

func triggerUpdates() []ResourceDef {
	return []ResourceDef{
		TriggerDef{
			Name:            "trigger1",
			ScheduleTrigger: "1 * * * *",
		},
	}
}

func expectedUpdatedTriggers() ResourceTests {
	return ResourceTests{
		TriggerTest{
			Name:            "trigger1",
			ScheduleTrigger: "1 * * * *",
			TaskIDs:         []int32{3, 13, 15},
			JobIDs:          []int32{4, 14, 16},
		},
	}
}

func testTriggerRemove(t *testing.T, originalTriggers ResourceTests, removeTrigger []TriggerDef, expectedRemovedTrigger []TriggerTest, removeResource []ResourceDef, expectedRemovedResource ResourceTests) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client, err := ctx.Create(t)
	defer ctx.Destroy()
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}
	names := originalTriggers.NameVariants()
	resources, err := getAll(client, TRIGGER, names)
	if err != nil {
		t.Fatalf("Failed to get all triggers: %v", names)
	}
	originalTriggers.Test(t, client, resources, true)

	// check that triggerRemove and remove have the same length
	if len(removeTrigger) != len(removeResource) {
		t.Fatalf("triggerRemove and remove must have the same length")
	}
	for i, u := range removeResource {
		if err := client.RemoveTrigger(context.Background(), removeTrigger[i], u); err != nil {
			t.Fatalf("Failed to remove resource: %v", err)
		}
		triggerNameVariant := NameVariant{Name: removeTrigger[i].Name}
		nameVariant := NameVariant{}
		switch def := u.(type) {
		case FeatureDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		case LabelDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		case SourceDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		case TrainingSetDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		default:
			t.Fatalf("Unrecognized resource type: %v", u)
		}
		actualResource, err := get(client, u.ResourceType(), nameVariant)
		if err != nil {
			t.Fatalf("Failed to get resource: %v", nameVariant)
		}
		expectedRemovedResource[i].Test(t, client, actualResource, true)

		actualTrigger, err := get(client, TRIGGER, triggerNameVariant)
		if err != nil {
			t.Fatalf("Failed to get resource: %v", triggerNameVariant)
		}
		expectedRemovedTrigger[i].Test(t, client, actualTrigger, true)
	}
}

func triggerRemove() []TriggerDef {
	return []TriggerDef{
		{
			Name:            "trigger1",
			ScheduleTrigger: "* * * * *",
			TaskIDs:         []int32{3, 13, 15},
			JobIDs:          []int32{4, 14, 16},
		},
		{
			Name:            "trigger2",
			ScheduleTrigger: "1 2 * * *",
			TaskIDs:         []int32{1, 5, 11},
			JobIDs:          []int32{2, 6, 12},
		},
	}
}

func resourceRemove() []ResourceDef {
	return []ResourceDef{
		LabelDef{
			Name:        "label",
			Variant:     "variant",
			Type:        "int64",
			Description: "label variant",
			Provider:    "mockOffline",
			Entity:      "user",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Other",
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Tags:       Tags{},
			Properties: Properties{},
			TaskIDs:    []int32{13},
			JobID:      14,
			Triggers:   []string{"trigger1"},
		},
		FeatureDef{
			Name:        "feature3",
			Variant:     "on-demand",
			Description: "Feature3 on-demand",
			Owner:       "Featureform",
			Location: PythonFunction{
				Query: []byte(PythonFunc),
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       CLIENT_COMPUTED,
			IsOnDemand: true,
			TaskIDs:    []int32{11},
			JobID:      12,
			Triggers:   []string{"trigger2"},
		},
	}
}

func expectedRemovedTriggers() []TriggerTest {
	return []TriggerTest{
		{
			Name:            "trigger1",
			ScheduleTrigger: "* * * * *",
			TaskIDs:         []int32{3, 15},
			JobIDs:          []int32{4, 16},
		},
		{
			Name:            "trigger2",
			ScheduleTrigger: "1 2 * * *",
			TaskIDs:         []int32{1, 5},
			JobIDs:          []int32{2, 6},
		},
	}
}

func expectedRemovedResources() ResourceTests {
	return ResourceTests{
		LabelVariantTest{
			Name:        "label",
			Variant:     "variant",
			Type:        "int64",
			Description: "label variant",
			Provider:    "mockOffline",
			Entity:      "user",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Other",
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			IsTable:  true,
			TaskIDs:  []int32{13},
			JobID:    14,
			Triggers: []string{},
		},
		FeatureVariantTest{
			Name:        "feature3",
			Variant:     "on-demand",
			Description: "Feature3 on-demand",
			Owner:       "Featureform",
			Location: PythonFunction{
				Query: []byte(PythonFunc),
			},
			Mode:       CLIENT_COMPUTED,
			IsOnDemand: true,
			TaskIDs:    []int32{11},
			JobID:      12,
			Triggers:   []string{},
		},
	}
}

func testTriggerAdd(t *testing.T, originalTriggers ResourceTests, addTrigger []TriggerDef, expectedAddedTrigger []TriggerTest, addResource []ResourceDef, expectedAddedResource ResourceTests) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client, err := ctx.Create(t)
	defer ctx.Destroy()
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}
	names := originalTriggers.NameVariants()
	resources, err := getAll(client, TRIGGER, names)
	if err != nil {
		t.Fatalf("Failed to get all triggers: %v", names)
	}
	originalTriggers.Test(t, client, resources, true)

	// check that triggerRemove and remove have the same length
	if len(addTrigger) != len(addResource) {
		t.Fatalf("triggerAdd and resourceAdd must have the same length")
	}
	for i, u := range addResource {
		if err := client.AddTrigger(context.Background(), addTrigger[i], u); err != nil {
			t.Fatalf("Failed to remove resource: %v", err)
		}
		triggerNameVariant := NameVariant{Name: addTrigger[i].Name}
		nameVariant := NameVariant{}
		switch def := u.(type) {
		case FeatureDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		case LabelDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		case SourceDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		case TrainingSetDef:
			nameVariant.Name = def.Name
			nameVariant.Variant = def.Variant
		default:
			t.Fatalf("Unrecognized resource type: %v", u)
		}
		actualResource, err := get(client, u.ResourceType(), nameVariant)
		if err != nil {
			t.Fatalf("Failed to get resource: %v", nameVariant)
		}
		expectedAddedResource[i].Test(t, client, actualResource, true)

		actualTrigger, err := get(client, TRIGGER, triggerNameVariant)
		if err != nil {
			t.Fatalf("Failed to get resource: %v", triggerNameVariant)
		}
		expectedAddedTrigger[i].Test(t, client, actualTrigger, true)
	}
}

func triggerAdd() []TriggerDef {
	return []TriggerDef{
		{
			Name:            "trigger1",
			ScheduleTrigger: "* * * * *",
			TaskIDs:         []int32{3, 13, 15},
			JobIDs:          []int32{4, 14, 16},
		},
		{
			Name:            "trigger2",
			ScheduleTrigger: "1 2 * * *",
			TaskIDs:         []int32{1, 5, 11},
			JobIDs:          []int32{2, 6, 12},
		},
	}
}

func resourceAdd() []ResourceDef {
	return []ResourceDef{
		FeatureDef{
			Name:        "feature3",
			Variant:     "on-demand",
			Description: "Feature3 on-demand",
			Owner:       "Featureform",
			Location: PythonFunction{
				Query: []byte(PythonFunc),
			},
			Tags:       Tags{},
			Properties: Properties{},
			Mode:       CLIENT_COMPUTED,
			IsOnDemand: true,
			TaskIDs:    []int32{11},
			JobID:      12,
			Triggers:   []string{"trigger2"},
		},
		TrainingSetDef{
			Name:        "training-set",
			Variant:     "variant",
			Provider:    "mockOffline",
			Description: "training-set variant",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature", "variant"},
				{"feature", "variant2"},
			},
			Owner:      "Other",
			Tags:       Tags{},
			Properties: Properties{},
			TaskIDs:    []int32{15},
			JobID:      16,
			Triggers:   []string{"trigger1"},
		},
	}
}

func expectedAddedTriggers() []TriggerTest {
	return []TriggerTest{
		{
			Name:            "trigger1",
			ScheduleTrigger: "* * * * *",
			TaskIDs:         []int32{3, 13, 15, 11},
			JobIDs:          []int32{4, 14, 16, 12},
		},
		{
			Name:            "trigger2",
			ScheduleTrigger: "1 2 * * *",
			TaskIDs:         []int32{1, 5, 11, 15},
			JobIDs:          []int32{2, 6, 12, 16},
		},
	}
}

func expectedAddedResources() ResourceTests {
	return ResourceTests{
		FeatureVariantTest{
			Name:        "feature3",
			Variant:     "on-demand",
			Description: "Feature3 on-demand",
			Owner:       "Featureform",
			Location: PythonFunction{
				Query: []byte(PythonFunc),
			},
			Mode:       CLIENT_COMPUTED,
			IsOnDemand: true,
			TaskIDs:    []int32{11},
			JobID:      12,
			Triggers:   []string{"trigger2", "trigger1"},
		},
		TrainingSetVariantTest{
			Name:        "training-set",
			Variant:     "variant",
			Provider:    "mockOffline",
			Description: "training-set variant",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature", "variant"},
				{"feature", "variant2"},
			},
			Owner:    "Other",
			TaskIDs:  []int32{15},
			JobID:    16,
			Triggers: []string{"trigger1", "trigger2"},
		},
	}
}

func TestTrigger(t *testing.T) {
	testListResources(t, TRIGGER, expectedTrigger())
	testGetResources(t, TRIGGER, expectedTrigger())
	testResourceUpdates(t, TRIGGER, expectedTrigger(), expectedUpdatedTriggers(), triggerUpdates())
	testTriggerRemove(t, expectedTrigger(), triggerRemove(), expectedRemovedTriggers(), resourceRemove(), expectedRemovedResources())
	testTriggerAdd(t, expectedTrigger(), triggerAdd(), expectedAddedTriggers(), resourceAdd(), expectedAddedResources())
}

type SourceTest ParentResourceTest

func (test SourceTest) NameVariant() NameVariant {
	return ParentResourceTest(test).NameVariant()
}

func (test SourceTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	ParentResourceTest(test).Test(t, client, res, shouldFetch)
	if shouldFetch {
		source := res.(*Source)
		variants, err := source.FetchVariants(client, context.Background())
		if err != nil {
			t.Fatalf("Failed to fetch variants: %s", err)
		}
		tests, err := expectedSourceVariants().Subset(source.NameVariants())
		if err != nil {
			t.Fatalf("Subset failed: %s", err)
		}
		// Don't fetch within a fetch to avoid an infinite loop.
		tests.Test(t, client, variants, false)
	}
}

func expectedSources() ResourceTests {
	return ResourceTests{
		SourceTest{
			Name:     "mockSource",
			Variants: []string{"var", "var2"},
			Default:  "var2",
		},
	}
}

func expectedSourceVariants() ResourceTests {
	return ResourceTests{
		SourceVariantTest{
			Name:        "mockSource",
			Variant:     "var",
			Description: "A CSV source",
			Owner:       "Featureform",
			Provider:    "mockOffline",
			Labels: []NameVariant{
				{"label", "variant"},
			},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
			IsTransformation:        true,
			IsSQLTransformation:     true,
			IsPrimaryData:           false,
			IsPrimaryDataSQLTable:   false,
			PrimaryDataSQLTableName: "",
			SQLTransformationSources: []NameVariant{{
				Name:    "mockName",
				Variant: "mockVariant",
			}},
			TaskIDs:  []int32{3},
			JobID:    4,
			Triggers: []string{"trigger2"},
		},
		SourceVariantTest{
			Name:        "mockSource",
			Variant:     "var2",
			Description: "A CSV source but different",
			Owner:       "Featureform",
			Provider:    "mockOffline",
			Labels:      []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant2"},
			},
			IsTransformation:         false,
			IsSQLTransformation:      false,
			IsPrimaryData:            true,
			IsPrimaryDataSQLTable:    true,
			PrimaryDataSQLTableName:  "mockPrimary",
			SQLTransformationSources: nil,
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
			TaskIDs:  []int32{3},
			JobID:    4,
			Triggers: []string{"trigger2"},
		},
	}
}

func TestSource(t *testing.T) {
	testListResources(t, SOURCE, expectedSources())
	testGetResources(t, SOURCE, expectedSources())
	testGetResources(t, SOURCE_VARIANT, expectedSourceVariants())
}

type FeatureTest ParentResourceTest

func (test FeatureTest) NameVariant() NameVariant {
	return ParentResourceTest(test).NameVariant()
}

func (test FeatureTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	ParentResourceTest(test).Test(t, client, res, shouldFetch)
	if shouldFetch {
		feature := res.(*Feature)
		variants, err := feature.FetchVariants(client, context.Background())
		if err != nil {
			t.Fatalf("Failed to fetch variants: %s", err)
		}
		tests, err := expectedFeatureVariants().Subset(feature.NameVariants())
		if err != nil {
			t.Fatalf("Subset failed: %s", err)
		}
		// Don't fetch within a fetch to avoid an infinite loop.
		tests.Test(t, client, variants, false)
	}
}

func expectedFeatures() ResourceTests {
	return ResourceTests{
		FeatureTest{
			Name:     "feature",
			Variants: []string{"variant", "variant2"},
			Default:  "variant2",
		},
		FeatureTest{
			Name:     "feature2",
			Variants: []string{"variant"},
			Default:  "variant",
		},
		FeatureTest{
			Name:     "feature3",
			Variants: []string{"on-demand"},
			Default:  "on-demand",
		},
	}
}

type FeatureVariantTest struct {
	Name         string
	Variant      string
	Description  string
	Type         string
	Owner        string
	Entity       string
	Provider     string
	Source       NameVariant
	TrainingSets []NameVariant
	Location     interface{}
	IsTable      bool
	Mode         ComputationMode
	IsOnDemand   bool
	TaskIDs      []int32
	JobID        int32
	Triggers     []string
}

func (test FeatureVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test FeatureVariantTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	t.Logf("Testing feature: %s %s", test.Name, test.Variant)
	feature := res.(*FeatureVariant)
	assertEqual(t, feature.Name(), test.Name)
	assertEqual(t, feature.Variant(), test.Variant)
	assertEqual(t, feature.Description(), test.Description)
	assertEqual(t, feature.Owner(), test.Owner)
	assertEqual(t, feature.TaskIDs(), test.TaskIDs)
	assertEqual(t, feature.JobID(), test.JobID)
	assertEquivalentStrings(t, feature.Triggers(), test.Triggers)
	if feature.Mode() == PRECOMPUTED {
		assertEqual(t, feature.Type(), test.Type)
		assertEqual(t, feature.Provider(), test.Provider)
		assertEqual(t, feature.Source(), test.Source)
		assertEqual(t, feature.Entity(), test.Entity)
		assertEqual(t, feature.isTable(), test.IsTable)
		assertEqual(t, feature.LocationColumns(), test.Location)
		if shouldFetch {
			testFetchProvider(t, client, feature)
			testFetchSource(t, client, feature)
			testFetchTrainingSets(t, client, feature)
		}
		assertEquivalentNameVariants(t, feature.TrainingSets(), test.TrainingSets)
	} else {
		assertEqual(t, feature.LocationFunction(), test.Location)
	}
	assertEqual(t, feature.Mode(), test.Mode)
	assertEqual(t, feature.Mode(), test.Mode)
	assertEqual(t, feature.IsOnDemand(), test.IsOnDemand)
	if tm := feature.Created(); tm == (time.Time{}) {
		t.Fatalf("Created time not set")
	}
}

func expectedFeatureVariants() ResourceTests {
	return ResourceTests{
		FeatureVariantTest{
			Name:        "feature",
			Variant:     "variant",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        "float",
			Description: "Feature variant",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Featureform",
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
			},
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			IsTable:  true,
			TaskIDs:  []int32{5},
			JobID:    6,
			Triggers: []string{"trigger2"},
		},
		FeatureVariantTest{
			Name:        "feature",
			Variant:     "variant2",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        "int",
			Description: "Feature variant2",
			Source:      NameVariant{"mockSource", "var2"},
			Owner:       "Featureform",
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			IsTable:  true,
			TaskIDs:  []int32{7},
			JobID:    8,
			Triggers: []string{"trigger3"},
		},
		FeatureVariantTest{
			Name:        "feature2",
			Variant:     "variant",
			Provider:    "mockOnline",
			Entity:      "user",
			Type:        "string",
			Description: "Feature2 variant",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Featureform",
			TrainingSets: []NameVariant{
				{"training-set", "variant2"},
			},
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			TaskIDs:  []int32{9},
			JobID:    10,
			IsTable:  true,
			Triggers: []string{"trigger3"},
		},
		FeatureVariantTest{
			Name:        "feature3",
			Variant:     "on-demand",
			Description: "Feature3 on-demand",
			Owner:       "Featureform",
			Location: PythonFunction{
				Query: []byte(PythonFunc),
			},
			Mode:       CLIENT_COMPUTED,
			IsOnDemand: true,
			TaskIDs:    []int32{11},
			JobID:      12,
			Triggers:   []string{"trigger2"},
		},
	}
}

func TestFeature(t *testing.T) {
	testListResources(t, FEATURE, expectedFeatures())
	testGetResources(t, FEATURE, expectedFeatures())
	testGetResources(t, FEATURE_VARIANT, expectedFeatureVariants())
}

type LabelTest ParentResourceTest

func (test LabelTest) NameVariant() NameVariant {
	return ParentResourceTest(test).NameVariant()
}

func (test LabelTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	ParentResourceTest(test).Test(t, client, res, shouldFetch)
	if shouldFetch {
		label := res.(*Label)
		variants, err := label.FetchVariants(client, context.Background())
		if err != nil {
			t.Fatalf("Failed to fetch variants: %s", err)
		}
		tests, err := expectedLabelVariants().Subset(label.NameVariants())
		if err != nil {
			t.Fatalf("Subset failed: %s", err)
		}
		// Don't fetch within a fetch to avoid an infinite loop.
		tests.Test(t, client, variants, false)
	}
}

func expectedLabels() ResourceTests {
	return ResourceTests{
		LabelTest{
			Name:     "label",
			Variants: []string{"variant"},
			Default:  "variant",
		},
	}
}

type LabelVariantTest struct {
	Name         string
	Variant      string
	Description  string
	Type         string
	Owner        string
	Entity       string
	Provider     string
	Source       NameVariant
	TrainingSets []NameVariant
	Location     ResourceVariantColumns
	IsTable      bool
	TaskIDs      []int32
	JobID        int32
	Triggers     []string
}

func (test LabelVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test LabelVariantTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	t.Logf("Testing label: %s %s", test.Name, test.Variant)
	label := res.(*LabelVariant)
	assertEqual(t, label.Name(), test.Name)
	assertEqual(t, label.Variant(), test.Variant)
	assertEqual(t, label.Description(), test.Description)
	assertEqual(t, label.Type(), test.Type)
	assertEqual(t, label.Owner(), test.Owner)
	assertEqual(t, label.Provider(), test.Provider)
	assertEqual(t, label.Source(), test.Source)
	assertEqual(t, label.Entity(), test.Entity)
	assertEqual(t, label.isTable(), test.IsTable)
	assertEqual(t, label.JobID(), test.JobID)
	assertEqual(t, label.TaskIDs(), test.TaskIDs)
	assertEquivalentStrings(t, label.Triggers(), test.Triggers)
	assertEquivalentNameVariants(t, label.TrainingSets(), test.TrainingSets)
	assertEquivalentNameVariants(t, label.TrainingSets(), test.TrainingSets)
	if shouldFetch {
		testFetchTrainingSets(t, client, label)
		testFetchSource(t, client, label)
		testFetchProvider(t, client, label)
	}
}

func expectedLabelVariants() ResourceTests {
	return ResourceTests{
		LabelVariantTest{
			Name:        "label",
			Variant:     "variant",
			Type:        "int64",
			Description: "label variant",
			Provider:    "mockOffline",
			Entity:      "user",
			Source:      NameVariant{"mockSource", "var"},
			Owner:       "Other",
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
			Location: ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			IsTable:  true,
			TaskIDs:  []int32{13},
			JobID:    14,
			Triggers: []string{"trigger1"},
		},
	}
}

func TestLabel(t *testing.T) {
	testListResources(t, LABEL, expectedLabels())
	testGetResources(t, LABEL, expectedLabels())
	testGetResources(t, LABEL_VARIANT, expectedLabelVariants())
}

type TrainingSetTest ParentResourceTest

func (test TrainingSetTest) NameVariant() NameVariant {
	return ParentResourceTest(test).NameVariant()
}

func (test TrainingSetTest) Test(t *testing.T, client *Client, res interface{}, shouldFetch bool) {
	ParentResourceTest(test).Test(t, client, res, shouldFetch)
	if shouldFetch {
		trainingSet := res.(*TrainingSet)
		variants, err := trainingSet.FetchVariants(client, context.Background())
		if err != nil {
			t.Fatalf("Failed to fetch variants: %s", err)
		}
		tests, err := expectedTrainingSetVariants().Subset(trainingSet.NameVariants())
		if err != nil {
			t.Fatalf("Subset failed: %s", err)
		}
		// Don't fetch within a fetch to avoid an infinite loop.
		tests.Test(t, client, variants, false)
	}
}

func expectedTrainingSets() ResourceTests {
	return ResourceTests{
		TrainingSetTest{
			Name:     "training-set",
			Variants: []string{"variant", "variant2"},
			Default:  "variant2",
		},
	}
}

type TrainingSetVariantTest struct {
	Name        string
	Variant     string
	Description string
	Owner       string
	Provider    string
	Label       NameVariant
	Features    []NameVariant
	TaskIDs     []int32
	JobID       int32
	Triggers    []string
}

func (test TrainingSetVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test TrainingSetVariantTest) Test(t *testing.T, client *Client, resource interface{}, shouldFetch bool) {
	t.Logf("Testing trainingSet: %s %s", test.Name, test.Variant)
	trainingSet := resource.(*TrainingSetVariant)
	assertEqual(t, trainingSet.Name(), test.Name)
	assertEqual(t, trainingSet.Variant(), test.Variant)
	assertEqual(t, trainingSet.Description(), test.Description)
	assertEqual(t, trainingSet.Owner(), test.Owner)
	assertEqual(t, trainingSet.Provider(), test.Provider)
	assertEqual(t, trainingSet.Label(), test.Label)
	assertEqual(t, trainingSet.TaskIDs(), test.TaskIDs)
	assertEqual(t, trainingSet.JobID(), test.JobID)
	assertEquivalentStrings(t, trainingSet.Triggers(), test.Triggers)
	assertEquivalentNameVariants(t, trainingSet.Features(), test.Features)
	if shouldFetch {
		testFetchProvider(t, client, trainingSet)
		testFetchLabel(t, client, trainingSet)
		testFetchFeatures(t, client, trainingSet)
	}
}

func expectedTrainingSetVariants() ResourceTests {
	return ResourceTests{
		TrainingSetVariantTest{
			Name:        "training-set",
			Variant:     "variant",
			Provider:    "mockOffline",
			Description: "training-set variant",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature", "variant"},
				{"feature", "variant2"},
			},
			Owner:    "Other",
			TaskIDs:  []int32{15},
			JobID:    16,
			Triggers: []string{"trigger1"},
		},
		TrainingSetVariantTest{
			Name:        "training-set",
			Variant:     "variant2",
			Provider:    "mockOffline",
			Description: "training-set variant2",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			Owner:    "Featureform",
			TaskIDs:  []int32{17},
			JobID:    18,
			Triggers: []string{"trigger3"},
		},
	}
}

func TestTrainingSet(t *testing.T) {
	testListResources(t, TRAINING_SET, expectedTrainingSets())
	testGetResources(t, TRAINING_SET, expectedTrainingSets())
	testGetResources(t, TRAINING_SET_VARIANT, expectedTrainingSetVariants())
}

type ModelTest struct {
	Name         string
	Description  string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
	Sources      []NameVariant
	Tags         Tags
	Properties   Properties
}

func (test ModelTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test ModelTest) Test(t *testing.T, client *Client, resource interface{}, shouldFetch bool) {
	t.Logf("Testing model: %s", test.Name)
	model := resource.(*Model)
	assertEqual(t, model.Name(), test.Name)
	assertEqual(t, model.Description(), test.Description)
	assertEqual(t, model.Tags(), test.Tags)
	assertEqual(t, model.Properties(), test.Properties)
	assertEquivalentNameVariants(t, model.Features(), test.Features)
	assertEquivalentNameVariants(t, model.Labels(), test.Labels)
	assertEquivalentNameVariants(t, model.TrainingSets(), test.TrainingSets)
	if shouldFetch {
		testFetchTrainingSets(t, client, model)
		testFetchLabels(t, client, model)
		testFetchFeatures(t, client, model)
	}
	if str := model.String(); str == "" {
		t.Fatalf("Invalid Model string: %s", str)
	}
}

func expectedModels() ResourceTests {
	return ResourceTests{
		ModelTest{
			Name:         "fraud",
			Description:  "fraud model",
			Labels:       []NameVariant{},
			Features:     []NameVariant{},
			TrainingSets: []NameVariant{},
			Tags:         []string{},
			Properties:   map[string]string{},
		},
	}
}

/*
Currently, the testing pattern assumes immutability, which made writing
test for model updates a bit awkward. As we roll out updates to other
resource types, we should consider refactoring the top-level interfaces
so we can more neatly encapsulate data/logic/etc. for updates.

For now, the below two functions work in tandem:
* `modelUpdates` holds 3 payloads that are applied in order
* `expectedUpdatedModels` holds the expected state after each payload is persisted
*/
func modelUpdates() []ResourceDef {
	return []ResourceDef{
		ModelDef{
			Name:        "fraud",
			Description: "fraud model",
			Features:    []NameVariant{},
			Trainingsets: []NameVariant{
				{Name: "training-set", Variant: "variant"},
			},
			Tags:       []string{"tag1"},
			Properties: map[string]string{"key1": "a"},
		},
		ModelDef{
			Name:        "fraud",
			Description: "fraud model",
			Features:    []NameVariant{},
			Trainingsets: []NameVariant{
				{Name: "training-set", Variant: "variant2"},
			},
			Tags:       []string{"tag2"},
			Properties: map[string]string{"key2": "b", "key3": "c"},
		},
		ModelDef{
			Name:        "fraud",
			Description: "fraud model",
			Features:    []NameVariant{},
			Trainingsets: []NameVariant{
				{Name: "training-set", Variant: "variant2"},
			},
			Tags:       []string{"tag2"},
			Properties: map[string]string{"key3": "d"},
		},
	}
}

func expectedUpdatedModels() ResourceTests {
	return ResourceTests{
		ModelTest{
			Name:        "fraud",
			Description: "fraud model",
			Labels:      []NameVariant{},
			Features:    []NameVariant{},
			TrainingSets: []NameVariant{
				{Name: "training-set", Variant: "variant"},
			},
			Tags:       []string{"tag1"},
			Properties: map[string]string{"key1": "a"},
		},
		ModelTest{
			Name:        "fraud",
			Description: "fraud model",
			Labels:      []NameVariant{},
			Features:    []NameVariant{},
			TrainingSets: []NameVariant{
				{Name: "training-set", Variant: "variant"},
				{Name: "training-set", Variant: "variant2"},
			},
			Tags:       []string{"tag1", "tag2"},
			Properties: map[string]string{"key1": "a", "key2": "b", "key3": "c"},
		},
		ModelTest{
			Name:        "fraud",
			Description: "fraud model",
			Labels:      []NameVariant{},
			Features:    []NameVariant{},
			TrainingSets: []NameVariant{
				{Name: "training-set", Variant: "variant"},
				{Name: "training-set", Variant: "variant2"},
			},
			Tags:       []string{"tag1", "tag2"},
			Properties: map[string]string{"key1": "a", "key2": "b", "key3": "d"},
		},
	}
}

func testResourceUpdates(t *testing.T, typ ResourceType, arranged, expected ResourceTests, updates []ResourceDef) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client, err := ctx.Create(t)
	defer ctx.Destroy()
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}
	names := arranged.NameVariants()
	resources, err := getAll(client, typ, names)
	if err != nil {
		t.Fatalf("Failed to get resources: %v", names)
	}
	arranged.Test(t, client, resources, true)

	for i, u := range updates {
		if err := update(client, typ, u); err != nil {
			t.Fatalf("Failed to update resource: %v", err)
		}
		nameVariant := NameVariant{}
		switch typ {
		case USER:
			nameVariant.Name = u.(UserDef).Name
		case MODEL:
			nameVariant.Name = u.(ModelDef).Name
		case PROVIDER:
			nameVariant.Name = u.(ProviderDef).Name
		case TRIGGER:
			nameVariant.Name = u.(TriggerDef).Name
		default:
			t.Fatalf("Unrecognized resource type: %v", typ)
		}
		actual, err := get(client, typ, nameVariant)
		if err != nil {
			t.Fatalf("Failed to get resource: %v", names[i])
		}
		expected[i].Test(t, client, actual, true)
	}
}

func TestModel(t *testing.T) {
	testListResources(t, MODEL, expectedModels())
	testGetResources(t, MODEL, expectedModels())
	testResourceUpdates(t, MODEL, expectedModels(), expectedUpdatedModels(), modelUpdates())
}

type ParentResourceTest struct {
	Name     string
	Variants []string
	Default  string
}

func (test ParentResourceTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test ParentResourceTest) Test(t *testing.T, client *Client, resource interface{}, shouldFetch bool) {
	t.Logf("Testing ParentResource: %s", test.Name)
	type ParentResource interface {
		Name() string
		Variants() []string
		NameVariants() NameVariants
		DefaultVariant() string
	}
	parentRes := resource.(ParentResource)
	assertEqual(t, parentRes.Name(), test.Name)
	assertEqual(t, parentRes.Variants(), test.Variants)
	assertEqual(t, parentRes.DefaultVariant(), test.Default)
	nameVars := make(NameVariants, len(test.Variants))
	for i, variant := range test.Variants {
		nameVars[i] = NameVariant{test.Name, variant}
	}
	assertEqual(t, parentRes.NameVariants(), nameVars)
}

type ResourceTest interface {
	NameVariant() NameVariant
	Test(t *testing.T, client *Client, resources interface{}, shouldFetch bool)
}

type ResourceTests []ResourceTest

func (tests ResourceTests) NameVariants() NameVariants {
	nameVars := make(NameVariants, len(tests))
	for i, test := range tests {
		nameVars[i] = test.NameVariant()
	}
	return nameVars
}

func (tests ResourceTests) Subset(nameVars []NameVariant) (ResourceTests, error) {
	testMap := tests.testMap()
	subset := make(ResourceTests, len(nameVars))
	for i, nameVar := range nameVars {
		var has bool
		subset[i], has = testMap[nameVar]
		if !has {
			return nil, fmt.Errorf("%+v not found in %+v", nameVar, testMap)
		}
	}
	return subset, nil
}

func (tests ResourceTests) testMap() map[NameVariant]ResourceTest {
	testMap := make(map[NameVariant]ResourceTest)
	for _, test := range tests {
		testMap[test.NameVariant()] = test
	}
	return testMap
}

func (tests ResourceTests) Test(t *testing.T, client *Client, resources interface{}, shouldFetch bool) {
	testMap := tests.testMap()
	type NameAndVariant interface {
		Name() string
		Variant() string
	}
	type NameOnly interface {
		Name() string
	}
	reflected := reflect.ValueOf(resources)
	for i := 0; i < reflected.Len(); i++ {
		var key NameVariant
		res := reflected.Index(i).Interface()
		switch casted := res.(type) {
		case NameAndVariant:
			key = NameVariant{casted.Name(), casted.Variant()}
		case NameOnly:
			key = NameVariant{Name: casted.Name()}
		default:
			panic("Resource doesn't implement Name()")
		}
		test, has := testMap[key]
		if !has {
			t.Fatalf("No test for Resource %v", key)
		}
		test.Test(t, client, res, shouldFetch)
		delete(testMap, key)
	}
	if len(testMap) != 0 {
		names := make([]NameVariant, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.NameVariant())
		}
		t.Fatalf("Resources not found %+v", names)
	}
}

func testGetResources(t *testing.T, typ ResourceType, tests ResourceTests) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client, err := ctx.Create(t)
	defer ctx.Destroy()
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}
	names := tests.NameVariants()
	resources, err := getAll(client, typ, names)
	if err != nil {
		t.Fatalf("Failed to get resources: %v", names)
	}
	tests.Test(t, client, resources, true)

	resource, err := get(client, typ, names[0])
	if err != nil {
		t.Fatalf("Failed to get resource: %v", names[0])
	}
	tests[0].Test(t, client, resource, true)

	noResources, err := getAll(client, typ, NameVariants{})
	if err != nil {
		t.Fatalf("Failed to get no resources")
	}
	if reflect.ValueOf(noResources).Len() != 0 {
		t.Fatalf("Got resources when expected none: %+v", noResources)
	}

	if res, err := get(client, typ, NameVariant{uuid.NewString(), uuid.NewString()}); err == nil {
		t.Fatalf("Succeeded in getting random resource: %+v", res)
	}
}

func testListResources(t *testing.T, typ ResourceType, tests ResourceTests) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client, err := ctx.Create(t)
	defer ctx.Destroy()
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}
	resources, err := list(client, typ)
	if err != nil {
		t.Fatalf("Failed to list resources: %v", resources)
	}
	tests.Test(t, client, resources, true)
}

type featuresFetcher interface {
	Features() NameVariants
	FetchFeatures(*Client, context.Context) ([]*FeatureVariant, error)
}

func testFetchFeatures(t *testing.T, client *Client, fetcher featuresFetcher) {
	tests, err := expectedFeatureVariants().Subset(fetcher.Features())
	if err != nil {
		t.Fatalf("Failed to get subset: %s", err)
	}
	features, err := fetcher.FetchFeatures(client, context.Background())
	if err != nil {
		t.Fatalf("Failed to fetch features: %s", err)
	}
	// Don't fetch when testing, otherwise we'll get an infinite loop of fetches
	tests.Test(t, client, features, false)
}

type labelsFetcher interface {
	Labels() NameVariants
	FetchLabels(*Client, context.Context) ([]*LabelVariant, error)
}

func testFetchLabels(t *testing.T, client *Client, fetcher labelsFetcher) {
	tests, err := expectedLabelVariants().Subset(fetcher.Labels())
	if err != nil {
		t.Fatalf("Failed to get subset: %s", err)
	}
	labels, err := fetcher.FetchLabels(client, context.Background())
	if err != nil {
		t.Fatalf("Failed to fetch labels: %s", err)
	}
	// Don't fetch when testing, otherwise we'll get an infinite loop of fetches
	tests.Test(t, client, labels, false)
}

type sourcesFetcher interface {
	Sources() NameVariants
	FetchSources(*Client, context.Context) ([]*SourceVariant, error)
}

func testFetchSources(t *testing.T, client *Client, fetcher sourcesFetcher) {
	tests, err := expectedSourceVariants().Subset(fetcher.Sources())
	if err != nil {
		t.Fatalf("Failed to get subset: %s", err)
	}
	sources, err := fetcher.FetchSources(client, context.Background())
	if err != nil {
		t.Fatalf("Failed to fetch sources: %s", err)
	}
	// Don't fetch when testing, otherwise we'll get an infinite loop of fetches
	tests.Test(t, client, sources, false)
}

type trainingSetsFetcher interface {
	TrainingSets() NameVariants
	FetchTrainingSets(*Client, context.Context) ([]*TrainingSetVariant, error)
}

func testFetchTrainingSets(t *testing.T, client *Client, fetcher trainingSetsFetcher) {
	tests, err := expectedTrainingSetVariants().Subset(fetcher.TrainingSets())
	if err != nil {
		t.Fatalf("Failed to get subset: %s", err)
	}
	trainingSets, err := fetcher.FetchTrainingSets(client, context.Background())
	if err != nil {
		t.Fatalf("Failed to fetch training sets: %s", err)
	}
	// Don't fetch when testing, otherwise we'll get an infinite loop of fetches
	tests.Test(t, client, trainingSets, false)
}

type labelFetcher interface {
	Label() NameVariant
	FetchLabel(*Client, context.Context) (*LabelVariant, error)
}

func testFetchLabel(t *testing.T, client *Client, fetcher labelFetcher) {
	tests, err := expectedLabelVariants().Subset(NameVariants{fetcher.Label()})
	if err != nil {
		t.Fatalf("Failed to get subset: %s", err)
	}
	label, err := fetcher.FetchLabel(client, context.Background())
	if err != nil {
		t.Fatalf("Failed to fetch label: %s", err)
	}
	// Don't fetch when testing, otherwise we'll get an infinite loop of fetches
	tests.Test(t, client, []*LabelVariant{label}, false)
}

type sourceFetcher interface {
	Source() NameVariant
	FetchSource(*Client, context.Context) (*SourceVariant, error)
}

func testFetchSource(t *testing.T, client *Client, fetcher sourceFetcher) {
	tests, err := expectedSourceVariants().Subset(NameVariants{fetcher.Source()})
	if err != nil {
		t.Fatalf("Failed to get subset: %s", err)
	}
	source, err := fetcher.FetchSource(client, context.Background())
	if err != nil {
		t.Fatalf("Failed to fetch source: %s", err)
	}
	// Don't fetch when testing, otherwise we'll get an infinite loop of fetches
	tests.Test(t, client, []*SourceVariant{source}, false)
}

type providerFetcher interface {
	Provider() string
	FetchProvider(*Client, context.Context) (*Provider, error)
}

func testFetchProvider(t *testing.T, client *Client, fetcher providerFetcher) {
	tests, err := expectedProviders().Subset(NameVariants{{Name: fetcher.Provider()}})
	if err != nil {
		t.Fatalf("Failed to get subset: %s", err)
	}
	provider, err := fetcher.FetchProvider(client, context.Background())
	if err != nil {
		t.Fatalf("Failed to fetch provider: %s", err)
	}
	// Don't fetch when testing, otherwise we'll get an infinite loop of fetches
	tests.Test(t, client, []*Provider{provider}, false)
}

func TestBannedStrings(t *testing.T) {
	resourceInvalidName := ResourceID{"nam__e", "variant", FEATURE}
	resourceInvalidVariant := ResourceID{"name", "varian__t", FEATURE}
	if err := resourceNamedSafely(resourceInvalidName); err == nil {
		t.Fatalf("testing didn't catch error on valid resource name")
	}
	if err := resourceNamedSafely(resourceInvalidVariant); err == nil {
		t.Fatalf("testing didn't catch error on valid resource name")
	}
	invalidNamePrefix := ResourceID{"_name", "variant", FEATURE}
	invalidVariantPrefix := ResourceID{"name", "_variant", FEATURE}
	if err := resourceNamedSafely(invalidNamePrefix); err == nil {
		t.Fatalf("testing didn't catch error on valid resource prefix")
	}
	if err := resourceNamedSafely(invalidVariantPrefix); err == nil {
		t.Fatalf("testing didn't catch error on valid variant prefix")
	}
	invalidNameSuffix := ResourceID{"name_", "variant", FEATURE}
	invalidVariantSuffix := ResourceID{"name", "variant_", FEATURE}
	if err := resourceNamedSafely(invalidNameSuffix); err == nil {
		t.Fatalf("testing didn't catch error on valid resource prefix")
	}
	if err := resourceNamedSafely(invalidVariantSuffix); err == nil {
		t.Fatalf("testing didn't catch error on valid variant prefix")
	}
	validName := ResourceID{"name", "variant", FEATURE}
	if err := resourceNamedSafely(validName); err != nil {
		t.Fatalf("valid resource triggered an error")
	}
}

func TestIsValidConfigUpdate(t *testing.T) {

	for _, providerType := range pt.AllProviderTypes {
		resource := &providerResource{
			serialized: &pb.Provider{
				Type: providerType.String(),
			},
		}

		_, err := resource.isValidConfigUpdate(pc.SerializedConfig{})
		if err != nil {
			if err.Error() == "config update not supported for provider type: "+providerType.String() {
				t.Fatalf("no support for provider type %s", providerType)
			}
		}
	}
}

type mocker struct {
}

func (mocker) GetProvider() string {
	return "test.provider"
}

func (mocker) GetCreated() *tspb.Timestamp {
	return tspb.Now()
}

func (mocker) GetLastUpdated() *tspb.Timestamp {
	return tspb.Now()
}

func (mocker) GetTags() *pb.Tags {
	return &pb.Tags{Tag: []string{"test.active", "test.inactive"}}
}

func (mocker) GetProperties() *pb.Properties {
	propertyMock := &pb.Properties{Property: map[string]*pb.Property{}}
	propertyMock.Property["test.map.key"] = &pb.Property{Value: &pb.Property_StringValue{StringValue: "test.map.value"}}
	return &pb.Properties{Property: propertyMock.Property}
}

func getSourceVariant() *SourceVariant {
	sv := &SourceVariant{
		serialized:           &pb.SourceVariant{Name: "test.name", Variant: "test.variant"},
		fetchFeaturesFns:     fetchFeaturesFns{},
		fetchLabelsFns:       fetchLabelsFns{},
		fetchProviderFns:     fetchProviderFns{getter: mocker{}},
		fetchTrainingSetsFns: fetchTrainingSetsFns{},
		createdFn:            createdFn{getter: mocker{}},
		lastUpdatedFn:        lastUpdatedFn{getter: mocker{}},
		fetchTagsFn:          fetchTagsFn{getter: mocker{}},
		fetchPropertiesFn:    fetchPropertiesFn{getter: mocker{}},
		protoStringer:        protoStringer{},
		fetchTriggersFn:      fetchTriggersFn{},
	}
	return sv
}

func Test_MetadataErrorInterceptors(t *testing.T) {
	_, addr := startServNoPanic(t)
	client := client(t, addr)
	context := context.Background()

	userDef := UserDef{
		Name:       "Featureform",
		Tags:       Tags{},
		Properties: Properties{},
	}

	sourceDef := SourceDef{
		Name:        "mockSource",
		Variant:     "var",
		Description: "A CSV source",
		Definition: TransformationSource{
			TransformationType: SQLTransformationType{
				Query: "SELECT * FROM dummy",
				Sources: []NameVariant{{
					Name:    "mockName",
					Variant: "mockVariant"},
				},
			},
		},
		Owner:      "Featureform",
		Provider:   "mockOffline",
		Tags:       Tags{},
		Properties: Properties{},
	}

	resourceDefs := []ResourceDef{userDef, sourceDef}

	err := client.CreateAll(context, resourceDefs)
	grpcErr, ok := grpc_status.FromError(err)
	if !ok {
		t.Fatalf("Expected error to be a grpc error")
	}
	if grpcErr == nil {
		t.Fatalf("Expected error to be non-nil")
	}

	// Test Streaming
	_, err = client.GetTrainingSet(context, "DNE")
	grpcErr, ok = grpc_status.FromError(err)
	if !ok {
		t.Fatalf("Expected error to be a grpc error")
	}
	if grpcErr == nil {
		t.Fatalf("Expected error to be non-nil")
	}

}

func TestSourceShallowMapOK(t *testing.T) {
	sv := getSourceVariant()

	sourceVariantResource := SourceShallowMap(sv)

	assertEqual(t, sv.serialized.Name, sourceVariantResource.Name)
	assertEqual(t, sv.serialized.Variant, sourceVariantResource.Variant)
	assertEqual(t, sv.Provider(), sourceVariantResource.Provider)
	assertEqual(t, len(sv.Tags()), len(sourceVariantResource.Tags))
	assertEqual(t, slices.Contains(sourceVariantResource.Tags, "test.active"), true)
	assertEqual(t, slices.Contains(sourceVariantResource.Tags, "test.inactive"), true)
	assertEqual(t, sv.Properties()["test.map.key"], sourceVariantResource.Properties["test.map.key"])
}

// TODO split these up into better tests
func Test_GetEquivalent(t *testing.T) {
	serv, addr := startServNoPanic(t)
	client := client(t, addr)
	context := context.Background()

	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "featureformer",
		Password:     "password",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "sysadmin",
	}
	userDef := UserDef{
		Name:       "Featureform",
		Tags:       Tags{},
		Properties: Properties{},
	}
	onlineDef := ProviderDef{
		Name:             "mockOnline",
		Description:      "A mock online provider",
		Type:             string(pt.RedisOnline),
		Software:         "redis",
		Team:             "fraud",
		SerializedConfig: redisConfig.Serialized(),
		Tags:             Tags{},
		Properties:       Properties{},
	}
	offlineDef := ProviderDef{
		Name:             "mockOffline",
		Description:      "A mock offline provider",
		Type:             string(pt.SnowflakeOffline),
		Software:         "snowflake",
		Team:             "recommendations",
		SerializedConfig: snowflakeConfig.Serialize(),
		Tags:             Tags{},
		Properties:       Properties{},
	}
	entityDef := EntityDef{
		Name:        "user",
		Description: "A user entity",
		Tags:        Tags{},
		Properties:  Properties{},
	}
	sourceDef := SourceDef{
		Name:        "mockSource",
		Variant:     "var",
		Description: "A CSV source",
		Definition: TransformationSource{
			TransformationType: SQLTransformationType{
				Query: "SELECT * FROM dummy",
				Sources: []NameVariant{{
					Name:    "mockName",
					Variant: "mockVariant"},
				},
			},
		},
		Owner:      "Featureform",
		Provider:   "mockOffline",
		Tags:       Tags{},
		Properties: Properties{},
	}
	featureDef := FeatureDef{
		Name:        "feature",
		Variant:     "variant",
		Description: "Feature3 on-demand",
		Owner:       "Featureform",
		Location: PythonFunction{
			Query: []byte(PythonFunc),
		},
		Tags:       Tags{},
		Properties: Properties{},
		Mode:       CLIENT_COMPUTED,
		IsOnDemand: true,
	}
	featureDef2 := FeatureDef{
		Name:        "feature2",
		Variant:     "variant",
		Description: "Feature3",
		Owner:       "Featureform",
		Source:      NameVariant{Name: "mockSource", Variant: "var"},
		Entity:      "user",
		Location: ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			TS:     "col3",
		},
		Tags:       Tags{},
		Properties: Properties{},
		Mode:       PRECOMPUTED,
		IsOnDemand: false,
	}
	labelDef := LabelDef{
		Name:        "label",
		Variant:     "variant",
		Type:        "int64",
		Description: "label variant",
		Provider:    "mockOffline",
		Entity:      "user",
		Source:      NameVariant{"mockSource", "var"},
		Owner:       "Featureform",
		Location: ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			TS:     "col3",
		},
		Tags:       Tags{},
		Properties: Properties{},
	}

	trainingSetDef := TrainingSetDef{
		Name:        "training-set",
		Variant:     "variant",
		Provider:    "mockOffline",
		Description: "training-set variant",
		Label:       NameVariant{"label", "variant"},
		Features: NameVariants{
			{"feature", "variant"},
			{"feature2", "variant"},
		},
		Owner:      "Featureform",
		Tags:       Tags{},
		Properties: Properties{},
	}

	defaultResourceVariant := &pb.ResourceVariant{}

	resourceDefs := []ResourceDef{userDef, entityDef, onlineDef, offlineDef, sourceDef, featureDef, featureDef2, labelDef, trainingSetDef}

	err := client.CreateAll(context, resourceDefs)
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}

	// sourceDef
	sourceDef.Description = "Some other description"
	sourceDef.Variant = "var2"
	svProto, err := sourceDef.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize source def: %s", err)
	}
	resourceVariant := &pb.ResourceVariant{Resource: &pb.ResourceVariant_SourceVariant{svProto}}
	equivalent, err := serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("There was an equivalent but we didn't get one")
	}

	sourceDef.Definition = TransformationSource{
		TransformationType: SQLTransformationType{
			Query: "SELECT count(*) FROM dummy",
			Sources: []NameVariant{{
				Name:    "mockName",
				Variant: "mockVariant"},
			},
		},
	}
	svProto2, err := sourceDef.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize source def: %s", err)
	}
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_SourceVariant{svProto2}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if !proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("There was no equivalent but we got one")
	}

	// labelDef
	labelDef.Description = "Some other description"
	lvProto, err := labelDef.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize label def: %s", err)
	}
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_LabelVariant{lvProto}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("There was an equivalent but we didn't get one")
	}

	// featureDef
	// on demand
	featureDef.Description = "Some other description"
	fvProto, err := featureDef.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize feature def: %s", err)
	}
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_FeatureVariant{fvProto}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("There was an equivalent but we didn't get one")
	}
	featureDef.Location = PythonFunction{
		Query: []byte("SELECT * FROM dummy"),
	}
	fvProto, err = featureDef.Serialize()
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_FeatureVariant{fvProto}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if !proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("there was no equivalent but we got one")
	}

	fvProto2, err := featureDef2.Serialize()
	fvProto2.Location = &pb.FeatureVariant_Columns{
		&pb.Columns{
			Entity: "col10",
			Value:  "col11",
			Ts:     "col12",
		},
	}
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_FeatureVariant{fvProto2}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if !proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("there was no equivalent but we got one")
	}

	// trainingSetDef
	trainingSetDef.Description = "Some other description"
	tsvProto := trainingSetDef.Serialize()
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_TrainingSetVariant{tsvProto}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("There was an equivalent but we didn't get one")
	}

	trainingSetDef.Features = NameVariants{
		{"feature", "variant"},
		{"feature2", "variant"},
		{"feature3", "variant"},
	}
	tsvProto = trainingSetDef.Serialize()
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_TrainingSetVariant{tsvProto}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if !proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("there was no equivalent but we got one")
	}

	trainingSetDef.Features = NameVariants{
		{"feature", "variant"},
		{"feature2", "variant"},
	}
	trainingSetDef.Label = NameVariant{"label_doesnt_exist", "variant"}
	tsvProto = trainingSetDef.Serialize()
	resourceVariant = &pb.ResourceVariant{Resource: &pb.ResourceVariant_TrainingSetVariant{tsvProto}}
	equivalent, err = serv.getEquivalent(resourceVariant, false)
	if err != nil {
		t.Fatalf("Failed to get equivalent: %s", err)
	}
	if !proto.Equal(equivalent, defaultResourceVariant) {
		t.Fatalf("there was no equivalent but we got one")
	}
}

// TODO split these up into better tests
func Test_CreateResourceVariantResourceChanged(t *testing.T) {
	_, addr := startServNoPanic(t)
	client := client(t, addr)
	context := context.Background()

	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "featureformer",
		Password:     "password",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "sysadmin",
	}
	userDef := UserDef{
		Name:       "Featureform",
		Tags:       Tags{},
		Properties: Properties{},
	}
	onlineDef := ProviderDef{
		Name:             "mockOnline",
		Description:      "A mock online provider",
		Type:             string(pt.RedisOnline),
		Software:         "redis",
		Team:             "fraud",
		SerializedConfig: redisConfig.Serialized(),
		Tags:             Tags{},
		Properties:       Properties{},
	}
	offlineDef := ProviderDef{
		Name:             "mockOffline",
		Description:      "A mock offline provider",
		Type:             string(pt.SnowflakeOffline),
		Software:         "snowflake",
		Team:             "recommendations",
		SerializedConfig: snowflakeConfig.Serialize(),
		Tags:             Tags{},
		Properties:       Properties{},
	}
	entityDef := EntityDef{
		Name:        "user",
		Description: "A user entity",
		Tags:        Tags{},
		Properties:  Properties{},
	}
	sourceDef := SourceDef{
		Name:        "mockSource",
		Variant:     "var",
		Description: "A CSV source",
		Definition: TransformationSource{
			TransformationType: SQLTransformationType{
				Query: "SELECT * FROM dummy",
				Sources: []NameVariant{{
					Name:    "mockName",
					Variant: "mockVariant"},
				},
			},
		},
		Owner:      "Featureform",
		Provider:   "mockOffline",
		Tags:       Tags{},
		Properties: Properties{},
	}
	featureDef := FeatureDef{
		Name:        "feature",
		Variant:     "variant",
		Description: "Feature3 on-demand",
		Owner:       "Featureform",
		Location: PythonFunction{
			Query: []byte(PythonFunc),
		},
		Tags:       Tags{},
		Properties: Properties{},
		Mode:       CLIENT_COMPUTED,
		IsOnDemand: true,
	}
	featureDef2 := FeatureDef{
		Name:        "feature2",
		Variant:     "variant",
		Description: "Feature3 on-demand",
		Owner:       "Featureform",
		Location: PythonFunction{
			Query: []byte(PythonFunc),
		},
		Tags:       Tags{},
		Properties: Properties{},
		Mode:       CLIENT_COMPUTED,
		IsOnDemand: true,
	}
	labelDef := LabelDef{
		Name:        "label",
		Variant:     "variant",
		Type:        "int64",
		Description: "label variant",
		Provider:    "mockOffline",
		Entity:      "user",
		Source:      NameVariant{"mockSource", "var"},
		Owner:       "Featureform",
		Location: ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			TS:     "col3",
		},
		Tags:       Tags{},
		Properties: Properties{},
	}

	trainingSetDef := TrainingSetDef{
		Name:        "training-set",
		Variant:     "variant",
		Provider:    "mockOffline",
		Description: "training-set variant",
		Label:       NameVariant{"label", "variant"},
		Features: NameVariants{
			{"feature", "variant"},
			{"feature2", "variant"},
		},
		Owner:      "Featureform",
		Tags:       Tags{},
		Properties: Properties{},
	}

	resourceDefs := []ResourceDef{userDef, entityDef, onlineDef, offlineDef, sourceDef, featureDef, featureDef2, labelDef, trainingSetDef}

	err := client.CreateAll(context, resourceDefs)
	if err != nil {
		t.Fatalf("Failed to create resources: %s", err)
	}

	// change sourceDef
	sourceDef.Definition = TransformationSource{
		TransformationType: SQLTransformationType{
			Query: "SELECT count(*) FROM dummy",
			Sources: []NameVariant{{
				Name:    "mockName",
				Variant: "mockVariant"},
			},
		},
	}
	err = client.Create(context, sourceDef)
	if err == nil {
		t.Fatalf("Expected error but got none")
	}

	// change labelDef
	labelDef.Source = NameVariant{"mockSource", "var2"}
	err = client.Create(context, labelDef)
	if err == nil {
		t.Fatalf("Expected error but got none")
	}

	// change featureDef
	featureDef.Location = PythonFunction{
		Query: []byte("def feature(): return 1"),
	}
	err = client.Create(context, featureDef)
	if err == nil {
		t.Fatalf("Expected error but got none")
	}

	// change trainingSetDef
	trainingSetDef.Features = NameVariants{
		{"feature", "variant"},
	}
	err = client.Create(context, trainingSetDef)
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
}
