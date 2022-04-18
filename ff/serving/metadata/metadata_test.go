package metadata

import (
	"context"
	"net"
	"reflect"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func filledResourceDefs() []ResourceDef {
	return []ResourceDef{
		UserDef{
			Name: "Featureform",
		},
		UserDef{
			Name: "Other",
		},
		ProviderDef{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             "REDIS-ONLINE",
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: []byte("ONLINE CONFIG"),
		},
		ProviderDef{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             "SNOWFLAKE-OFFLINE",
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: []byte("OFFLINE CONFIG"),
		},
		EntityDef{
			Name:        "user",
			Description: "A user entity",
		},
		EntityDef{
			Name:        "item",
			Description: "An item entity",
		},
		SourceDef{
			Name:        "mockSource",
			Variant:     "var",
			Description: "A CSV source",
			Type:        "csv",
			Owner:       "Featureform",
			Provider:    "mockOffline",
		},
		SourceDef{
			Name:        "mockSource",
			Variant:     "var2",
			Description: "A CSV source but different",
			Type:        "csv",
			Owner:       "Featureform",
			Provider:    "mockOffline",
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
			Owner: "Other",
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
			Owner: "Featureform",
		},
		ModelDef{
			Name:        "fraud",
			Description: "fraud model",
		},
	}
}

type testContext struct {
	Defs   []ResourceDef
	serv   *MetadataServer
	client *Client
}

func (ctx *testContext) Create(t *testing.T) *Client {
	var addr string
	ctx.serv, addr = startServ()
	ctx.client = client(t, addr)
	if err := ctx.client.CreateAll(context.Background(), ctx.Defs); err != nil {
		t.Fatalf("Failed to create: %s", err)
	}
	return ctx.client
}

func (ctx *testContext) Destroy() {
	ctx.serv.Stop()
	ctx.client.Close()
}

func startServ() (*MetadataServer, string) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
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

func client(t *testing.T, addr string) *Client {
	logger := zaptest.NewLogger(t).Sugar()
	client, err := NewClient(addr, logger)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	return client
}

func TestCreate(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	ctx.Create(t)
	defer ctx.Destroy()
}

func assertEqual(t *testing.T, this, that interface{}) {
	t.Helper()
	if !reflect.DeepEqual(this, that) {
		t.Fatalf("Values not equal\nActual: %+v\nExpected: %+v", this, that)
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

type UserTests []UserTest

func (tests UserTests) Test(t *testing.T, client *Client, users []*User) {
	testMap := make(map[string]UserTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, user := range users {
		test, has := testMap[user.Name()]
		if !has {
			t.Fatalf("No test for User %s", user.Name())
		}
		test.Test(t, client, user)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("Users not found %+v", names)
	}
}

type UserTest struct {
	Name         string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
	Sources      []NameVariant
}

func (test UserTest) Test(t *testing.T, client *Client, user *User) {
	assertEqual(t, user.Name(), test.Name)
	assertEquivalentNameVariants(t, user.Features(), test.Features)
	assertEquivalentNameVariants(t, user.Labels(), test.Labels)
	assertEquivalentNameVariants(t, user.TrainingSets(), test.TrainingSets)
	assertEquivalentNameVariants(t, user.Sources(), test.Sources)
}

func expectedUsers() UserTests {
	return UserTests{
		{
			Name:   "Featureform",
			Labels: []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			Sources: []NameVariant{
				{"mockSource", "var"},
				{"mockSource", "var2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant2"},
			},
		},
		{
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

func TestListUsers(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	users, err := client.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("Failed to list users: %v", users)
	}
	expectedUsers().Test(t, client, users)
}

func TestGetUsers(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedUsers()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	users, err := client.GetUsers(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get users: %v", names)
	}
	exp.Test(t, client, users)

	user, err := client.GetUser(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get user: %v", names[0])
	}
	exp[0].Test(t, client, user)

	noUsers, err := client.GetUsers(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no users")
	}
	if len(noUsers) != 0 {
		t.Fatalf("Got users when expected none: %+v", noUsers)
	}
}

type ProviderTests []ProviderTest

func (tests ProviderTests) Test(t *testing.T, client *Client, providers []*Provider) {
	testMap := make(map[string]ProviderTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, provider := range providers {
		test, has := testMap[provider.Name()]
		if !has {
			t.Fatalf("No test for Provider %s", provider.Name())
		}
		test.Test(t, client, provider)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("Providers not found %+v", names)
	}
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
}

func (test ProviderTest) Test(t *testing.T, client *Client, provider *Provider) {
	assertEqual(t, provider.Name(), test.Name)
	assertEqual(t, provider.Team(), test.Team)
	assertEqual(t, provider.Type(), test.Type)
	assertEqual(t, provider.Description(), test.Description)
	assertEqual(t, provider.Software(), test.Software)
	assertEqual(t, provider.SerializedConfig(), test.SerializedConfig)
	assertEquivalentNameVariants(t, provider.Features(), test.Features)
	assertEquivalentNameVariants(t, provider.Labels(), test.Labels)
	assertEquivalentNameVariants(t, provider.TrainingSets(), test.TrainingSets)
	assertEquivalentNameVariants(t, provider.Sources(), test.Sources)
}

func expectedProviders() ProviderTests {
	return ProviderTests{
		{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             "REDIS-ONLINE",
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: []byte("ONLINE CONFIG"),
			Labels:           []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant"},
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			Sources:      []NameVariant{},
			TrainingSets: []NameVariant{},
		},
		{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             "SNOWFLAKE-OFFLINE",
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: []byte("OFFLINE CONFIG"),
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
		},
	}
}

func TestListProviders(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	providers, err := client.ListProviders(context.Background())
	if err != nil {
		t.Fatalf("Failed to list providers: %v", providers)
	}
	expectedProviders().Test(t, client, providers)
}

func TestGetProviders(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedProviders()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	providers, err := client.GetProviders(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get providers: %v", names)
	}
	exp.Test(t, client, providers)

	provider, err := client.GetProvider(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get provider: %v", names[0])
	}
	exp[0].Test(t, client, provider)

	noProviders, err := client.GetProviders(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no providers")
	}
	if len(noProviders) != 0 {
		t.Fatalf("Got providers when expected none: %+v", noProviders)
	}
}

type EntityTests []EntityTest

func (tests EntityTests) Test(t *testing.T, client *Client, entities []*Entity) {
	testMap := make(map[string]EntityTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, entity := range entities {
		test, has := testMap[entity.Name()]
		if !has {
			t.Fatalf("No test for Entity %s", entity.Name())
		}
		test.Test(t, client, entity)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("Entities not found %+v", names)
	}
}

type EntityTest struct {
	Name         string
	Description  string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
	Sources      []NameVariant
}

func (test EntityTest) Test(t *testing.T, client *Client, entity *Entity) {
	t.Logf("Testing entity: %s", test.Name)
	assertEqual(t, entity.Name(), test.Name)
	assertEqual(t, entity.Description(), test.Description)
	assertEquivalentNameVariants(t, entity.Features(), test.Features)
	assertEquivalentNameVariants(t, entity.Labels(), test.Labels)
	assertEquivalentNameVariants(t, entity.TrainingSets(), test.TrainingSets)
}

func expectedEntities() EntityTests {
	return EntityTests{
		{
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
		{
			Name:         "item",
			Description:  "An item entity",
			Labels:       []NameVariant{},
			Features:     []NameVariant{},
			TrainingSets: []NameVariant{},
		},
	}
}

func TestListEntities(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	entities, err := client.ListEntities(context.Background())
	if err != nil {
		t.Fatalf("Failed to list entities: %v", entities)
	}
	expectedEntities().Test(t, client, entities)
}

func TestGetEntities(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedEntities()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	entities, err := client.GetEntities(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get entities: %v", names)
	}
	exp.Test(t, client, entities)

	entity, err := client.GetEntity(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get entity: %v", names[0])
	}
	exp[0].Test(t, client, entity)

	noEntities, err := client.GetEntities(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no entities")
	}
	if len(noEntities) != 0 {
		t.Fatalf("Got entities when expected none: %+v", noEntities)
	}
}

type SourceTests []SourceTest

func (tests SourceTests) Test(t *testing.T, client *Client, sources []*Source) {
	testMap := make(map[string]SourceTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, source := range sources {
		test, has := testMap[source.Name()]
		if !has {
			t.Fatalf("No test for Source %s", source.Name())
		}
		test.Test(t, client, source)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("Sources not found %+v", names)
	}
}

type SourceTest struct {
	Name     string
	Variants []string
	Default  string
}

func (test SourceTest) Test(t *testing.T, client *Client, source *Source) {
	t.Logf("Testing source: %s", test.Name)
	assertEqual(t, source.Name(), test.Name)
	assertEqual(t, source.Variants(), test.Variants)
	assertEqual(t, source.DefaultVariant(), test.Default)
}

func expectedSources() SourceTests {
	return SourceTests{
		{
			Name:     "mockSource",
			Variants: []string{"var", "var2"},
			Default:  "var",
		},
	}
}

type SourceVariantTests []SourceVariantTest

func (tests SourceVariantTests) Test(t *testing.T, client *Client, sources []*SourceVariant) {
	testMap := make(map[NameVariant]SourceVariantTest)
	for _, test := range tests {
		testMap[NameVariant{test.Name, test.Variant}] = test
	}
	for _, source := range sources {
		test, has := testMap[NameVariant{source.Name(), source.Variant()}]
		if !has {
			t.Fatalf("No test for SourceVariant %s %s", source.Name(), source.Variant())
		}
		test.Test(t, client, source)
		delete(testMap, NameVariant{test.Name, test.Variant})
	}
	if len(testMap) != 0 {
		names := make([]NameVariant, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, NameVariant{test.Name, test.Variant})
		}
		t.Fatalf("SourceVariants not found %+v", names)
	}
}

type SourceVariantTest struct {
	Name         string
	Variant      string
	Description  string
	Type         string
	Owner        string
	Provider     string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
}

func (test SourceVariantTest) Test(t *testing.T, client *Client, source *SourceVariant) {
	t.Logf("Testing source: %s %s", test.Name, test.Variant)
	assertEqual(t, source.Name(), test.Name)
	assertEqual(t, source.Variant(), test.Variant)
	assertEqual(t, source.Description(), test.Description)
	assertEqual(t, source.Type(), test.Type)
	assertEqual(t, source.Owner(), test.Owner)
	assertEqual(t, source.Provider(), test.Provider)
	assertEquivalentNameVariants(t, source.Features(), test.Features)
	assertEquivalentNameVariants(t, source.Labels(), test.Labels)
	assertEquivalentNameVariants(t, source.TrainingSets(), test.TrainingSets)
}

func expectedSourceVariants() SourceVariantTests {
	return SourceVariantTests{
		{
			Name:        "mockSource",
			Variant:     "var",
			Description: "A CSV source",
			Type:        "csv",
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
		},
		{
			Name:        "mockSource",
			Variant:     "var2",
			Description: "A CSV source but different",
			Type:        "csv",
			Owner:       "Featureform",
			Provider:    "mockOffline",
			Labels:      []NameVariant{},
			Features: []NameVariant{
				{"feature", "variant2"},
			},
			TrainingSets: []NameVariant{
				{"training-set", "variant"},
				{"training-set", "variant2"},
			},
		},
	}
}

func TestListSourceVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	sources, err := client.ListSources(context.Background())
	if err != nil {
		t.Fatalf("Failed to list sources: %v", sources)
	}
	expectedSources().Test(t, client, sources)
}

func TestGetSources(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedSources()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	sources, err := client.GetSources(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get sources: %v", names)
	}
	exp.Test(t, client, sources)

	source, err := client.GetSource(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get source: %v", names[0])
	}
	exp[0].Test(t, client, source)

	noSources, err := client.GetSources(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no sources")
	}
	if len(noSources) != 0 {
		t.Fatalf("Got sources when expected none: %+v", noSources)
	}
}

func TestGetSourceVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedSourceVariants()
	names := make([]NameVariant, len(exp))
	for i, e := range exp {
		names[i] = NameVariant{e.Name, e.Variant}
	}
	sources, err := client.GetSourceVariants(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get sources: %v", names)
	}
	exp.Test(t, client, sources)

	source, err := client.GetSourceVariant(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get source: %v", names[0])
	}
	exp[0].Test(t, client, source)

	noSourceVariants, err := client.GetSourceVariants(context.Background(), []NameVariant{})
	if err != nil {
		t.Fatalf("Failed to get no sources")
	}
	if len(noSourceVariants) != 0 {
		t.Fatalf("Got sources when expected none: %+v", noSourceVariants)
	}
}

type FeatureTests []FeatureTest

func (tests FeatureTests) Test(t *testing.T, client *Client, features []*Feature) {
	testMap := make(map[string]FeatureTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, feature := range features {
		test, has := testMap[feature.Name()]
		if !has {
			t.Fatalf("No test for Feature %s", feature.Name())
		}
		test.Test(t, client, feature)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("Features not found %+v", names)
	}
}

type FeatureTest struct {
	Name     string
	Variants []string
	Default  string
}

func (test FeatureTest) Test(t *testing.T, client *Client, feature *Feature) {
	t.Logf("Testing feature: %s", test.Name)
	assertEqual(t, feature.Name(), test.Name)
	assertEqual(t, feature.Variants(), test.Variants)
	assertEqual(t, feature.DefaultVariant(), test.Default)
}

func expectedFeatures() FeatureTests {
	return FeatureTests{
		{
			Name:     "feature",
			Variants: []string{"variant", "variant2"},
			Default:  "variant",
		},
		{
			Name:     "feature2",
			Variants: []string{"variant"},
			Default:  "variant",
		},
	}
}

type FeatureVariantTests []FeatureVariantTest

func (tests FeatureVariantTests) Test(t *testing.T, client *Client, features []*FeatureVariant) {
	testMap := make(map[NameVariant]FeatureVariantTest)
	for _, test := range tests {
		testMap[NameVariant{test.Name, test.Variant}] = test
	}
	for _, feature := range features {
		test, has := testMap[NameVariant{feature.Name(), feature.Variant()}]
		if !has {
			t.Fatalf("No test for FeatureVariant %s %s", feature.Name(), feature.Variant())
		}
		test.Test(t, client, feature)
		delete(testMap, NameVariant{test.Name, test.Variant})
	}
	if len(testMap) != 0 {
		names := make([]NameVariant, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, NameVariant{test.Name, test.Variant})
		}
		t.Fatalf("FeatureVariants not found %+v", names)
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
}

func (test FeatureVariantTest) Test(t *testing.T, client *Client, feature *FeatureVariant) {
	t.Logf("Testing feature: %s %s", test.Name, test.Variant)
	assertEqual(t, feature.Name(), test.Name)
	assertEqual(t, feature.Variant(), test.Variant)
	assertEqual(t, feature.Description(), test.Description)
	assertEqual(t, feature.Type(), test.Type)
	assertEqual(t, feature.Owner(), test.Owner)
	assertEqual(t, feature.Provider(), test.Provider)
	assertEqual(t, feature.Source(), test.Source)
	assertEqual(t, feature.Entity(), test.Entity)
	assertEquivalentNameVariants(t, feature.TrainingSets(), test.TrainingSets)
}

func expectedFeatureVariants() FeatureVariantTests {
	return FeatureVariantTests{
		{
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
		},
		{
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
		},
		{
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
		},
	}
}

func TestListFeatureVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	features, err := client.ListFeatures(context.Background())
	if err != nil {
		t.Fatalf("Failed to list features: %v", features)
	}
	expectedFeatures().Test(t, client, features)
}

func TestGetFeatures(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedFeatures()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	features, err := client.GetFeatures(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get features: %v", names)
	}
	exp.Test(t, client, features)

	feature, err := client.GetFeature(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get feature: %v", names[0])
	}
	exp[0].Test(t, client, feature)

	noFeatures, err := client.GetFeatures(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no features")
	}
	if len(noFeatures) != 0 {
		t.Fatalf("Got features when expected none: %+v", noFeatures)
	}
}

func TestGetFeatureVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedFeatureVariants()
	names := make([]NameVariant, len(exp))
	for i, e := range exp {
		names[i] = NameVariant{e.Name, e.Variant}
	}
	features, err := client.GetFeatureVariants(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get features: %v", names)
	}
	exp.Test(t, client, features)

	feature, err := client.GetFeatureVariant(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get feature: %v", names[0])
	}
	exp[0].Test(t, client, feature)

	noFeatureVariants, err := client.GetFeatureVariants(context.Background(), []NameVariant{})
	if err != nil {
		t.Fatalf("Failed to get no features")
	}
	if len(noFeatureVariants) != 0 {
		t.Fatalf("Got features when expected none: %+v", noFeatureVariants)
	}
}

type LabelTests []LabelTest

func (tests LabelTests) Test(t *testing.T, client *Client, labels []*Label) {
	testMap := make(map[string]LabelTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, label := range labels {
		test, has := testMap[label.Name()]
		if !has {
			t.Fatalf("No test for Label %s", label.Name())
		}
		test.Test(t, client, label)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("Labels not found %+v", names)
	}
}

type LabelTest struct {
	Name     string
	Variants []string
	Default  string
}

func (test LabelTest) Test(t *testing.T, client *Client, label *Label) {
	t.Logf("Testing label: %s", test.Name)
	assertEqual(t, label.Name(), test.Name)
	assertEqual(t, label.Variants(), test.Variants)
	assertEqual(t, label.DefaultVariant(), test.Default)
}

func expectedLabels() LabelTests {
	return LabelTests{
		{
			Name:     "label",
			Variants: []string{"variant"},
			Default:  "variant",
		},
	}
}

type LabelVariantTests []LabelVariantTest

func (tests LabelVariantTests) Test(t *testing.T, client *Client, labels []*LabelVariant) {
	testMap := make(map[NameVariant]LabelVariantTest)
	for _, test := range tests {
		testMap[NameVariant{test.Name, test.Variant}] = test
	}
	for _, label := range labels {
		test, has := testMap[NameVariant{label.Name(), label.Variant()}]
		if !has {
			t.Fatalf("No test for LabelVariant %s %s", label.Name(), label.Variant())
		}
		test.Test(t, client, label)
		delete(testMap, NameVariant{test.Name, test.Variant})
	}
	if len(testMap) != 0 {
		names := make([]NameVariant, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, NameVariant{test.Name, test.Variant})
		}
		t.Fatalf("LabelVariants not found %+v", names)
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
}

func (test LabelVariantTest) Test(t *testing.T, client *Client, label *LabelVariant) {
	t.Logf("Testing label: %s %s", test.Name, test.Variant)
	assertEqual(t, label.Name(), test.Name)
	assertEqual(t, label.Variant(), test.Variant)
	assertEqual(t, label.Description(), test.Description)
	assertEqual(t, label.Type(), test.Type)
	assertEqual(t, label.Owner(), test.Owner)
	assertEqual(t, label.Provider(), test.Provider)
	assertEqual(t, label.Source(), test.Source)
	assertEqual(t, label.Entity(), test.Entity)
	assertEquivalentNameVariants(t, label.TrainingSets(), test.TrainingSets)
}

func expectedLabelVariants() LabelVariantTests {
	return LabelVariantTests{
		{
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
		},
	}
}

func TestListLabelVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	labels, err := client.ListLabels(context.Background())
	if err != nil {
		t.Fatalf("Failed to list labels: %v", labels)
	}
	expectedLabels().Test(t, client, labels)
}

func TestGetLabels(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedLabels()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	labels, err := client.GetLabels(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get labels: %v", names)
	}
	exp.Test(t, client, labels)

	label, err := client.GetLabel(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get label: %v", names[0])
	}
	exp[0].Test(t, client, label)

	noLabels, err := client.GetLabels(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no labels")
	}
	if len(noLabels) != 0 {
		t.Fatalf("Got labels when expected none: %+v", noLabels)
	}
}

func TestGetLabelVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedLabelVariants()
	names := make([]NameVariant, len(exp))
	for i, e := range exp {
		names[i] = NameVariant{e.Name, e.Variant}
	}
	labels, err := client.GetLabelVariants(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get labels: %v", names)
	}
	exp.Test(t, client, labels)

	label, err := client.GetLabelVariant(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get label: %v", names[0])
	}
	exp[0].Test(t, client, label)

	noLabelVariants, err := client.GetLabelVariants(context.Background(), []NameVariant{})
	if err != nil {
		t.Fatalf("Failed to get no labels")
	}
	if len(noLabelVariants) != 0 {
		t.Fatalf("Got labels when expected none: %+v", noLabelVariants)
	}
}

type TrainingSetTests []TrainingSetTest

func (tests TrainingSetTests) Test(t *testing.T, client *Client, trainingSets []*TrainingSet) {
	testMap := make(map[string]TrainingSetTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, trainingSet := range trainingSets {
		test, has := testMap[trainingSet.Name()]
		if !has {
			t.Fatalf("No test for TrainingSet %s", trainingSet.Name())
		}
		test.Test(t, client, trainingSet)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("TrainingSets not found %+v", names)
	}
}

type TrainingSetTest struct {
	Name     string
	Variants []string
	Default  string
}

func (test TrainingSetTest) Test(t *testing.T, client *Client, trainingSet *TrainingSet) {
	t.Logf("Testing trainingSet: %s", test.Name)
	assertEqual(t, trainingSet.Name(), test.Name)
	assertEqual(t, trainingSet.Variants(), test.Variants)
	assertEqual(t, trainingSet.DefaultVariant(), test.Default)
}

func expectedTrainingSets() TrainingSetTests {
	return TrainingSetTests{
		{
			Name:     "training-set",
			Variants: []string{"variant", "variant2"},
			Default:  "variant",
		},
	}
}

type TrainingSetVariantTests []TrainingSetVariantTest

func (tests TrainingSetVariantTests) Test(t *testing.T, client *Client, trainingSets []*TrainingSetVariant) {
	testMap := make(map[NameVariant]TrainingSetVariantTest)
	for _, test := range tests {
		testMap[NameVariant{test.Name, test.Variant}] = test
	}
	for _, trainingSet := range trainingSets {
		test, has := testMap[NameVariant{trainingSet.Name(), trainingSet.Variant()}]
		if !has {
			t.Fatalf("No test for TrainingSetVariant %s %s", trainingSet.Name(), trainingSet.Variant())
		}
		test.Test(t, client, trainingSet)
		delete(testMap, NameVariant{test.Name, test.Variant})
	}
	if len(testMap) != 0 {
		names := make([]NameVariant, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, NameVariant{test.Name, test.Variant})
		}
		t.Fatalf("TrainingSetVariants not found %+v", names)
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
}

func (test TrainingSetVariantTest) Test(t *testing.T, client *Client, trainingSet *TrainingSetVariant) {
	t.Logf("Testing trainingSet: %s %s", test.Name, test.Variant)
	assertEqual(t, trainingSet.Name(), test.Name)
	assertEqual(t, trainingSet.Variant(), test.Variant)
	assertEqual(t, trainingSet.Description(), test.Description)
	assertEqual(t, trainingSet.Owner(), test.Owner)
	assertEqual(t, trainingSet.Provider(), test.Provider)
	assertEqual(t, trainingSet.Label(), test.Label)
	assertEquivalentNameVariants(t, trainingSet.Features(), test.Features)
}

func expectedTrainingSetVariants() TrainingSetVariantTests {
	return TrainingSetVariantTests{
		{
			Name:        "training-set",
			Variant:     "variant",
			Provider:    "mockOffline",
			Description: "training-set variant",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature", "variant"},
				{"feature", "variant2"},
			},
			Owner: "Other",
		},
		{
			Name:        "training-set",
			Variant:     "variant2",
			Provider:    "mockOffline",
			Description: "training-set variant2",
			Label:       NameVariant{"label", "variant"},
			Features: NameVariants{
				{"feature2", "variant"},
				{"feature", "variant2"},
			},
			Owner: "Featureform",
		},
	}
}

func TestListTrainingSetVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	trainingSets, err := client.ListTrainingSets(context.Background())
	if err != nil {
		t.Fatalf("Failed to list trainingSets: %v", trainingSets)
	}
	expectedTrainingSets().Test(t, client, trainingSets)
}

func TestGetTrainingSets(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedTrainingSets()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	trainingSets, err := client.GetTrainingSets(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get trainingSets: %v", names)
	}
	exp.Test(t, client, trainingSets)

	trainingSet, err := client.GetTrainingSet(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get trainingSet: %v", names[0])
	}
	exp[0].Test(t, client, trainingSet)

	noTrainingSets, err := client.GetTrainingSets(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no trainingSets")
	}
	if len(noTrainingSets) != 0 {
		t.Fatalf("Got trainingSets when expected none: %+v", noTrainingSets)
	}
}

func TestGetTrainingSetVariants(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedTrainingSetVariants()
	names := make([]NameVariant, len(exp))
	for i, e := range exp {
		names[i] = NameVariant{e.Name, e.Variant}
	}
	trainingSets, err := client.GetTrainingSetVariants(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get trainingSets: %v", names)
	}
	exp.Test(t, client, trainingSets)

	trainingSet, err := client.GetTrainingSetVariant(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get trainingSet: %v", names[0])
	}
	exp[0].Test(t, client, trainingSet)

	noTrainingSetVariants, err := client.GetTrainingSetVariants(context.Background(), []NameVariant{})
	if err != nil {
		t.Fatalf("Failed to get no trainingSets")
	}
	if len(noTrainingSetVariants) != 0 {
		t.Fatalf("Got trainingSets when expected none: %+v", noTrainingSetVariants)
	}
}

type ModelTests []ModelTest

func (tests ModelTests) Test(t *testing.T, client *Client, models []*Model) {
	testMap := make(map[string]ModelTest)
	for _, test := range tests {
		testMap[test.Name] = test
	}
	for _, model := range models {
		test, has := testMap[model.Name()]
		if !has {
			t.Fatalf("No test for Model %s", model.Name())
		}
		test.Test(t, client, model)
		delete(testMap, test.Name)
	}
	if len(testMap) != 0 {
		names := make([]string, 0, len(testMap))
		for _, test := range testMap {
			names = append(names, test.Name)
		}
		t.Fatalf("Models not found %+v", names)
	}
}

type ModelTest struct {
	Name         string
	Description  string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
	Sources      []NameVariant
}

func (test ModelTest) Test(t *testing.T, client *Client, model *Model) {
	t.Logf("Testing model: %s", test.Name)
	assertEqual(t, model.Name(), test.Name)
	assertEqual(t, model.Description(), test.Description)
	assertEquivalentNameVariants(t, model.Features(), test.Features)
	assertEquivalentNameVariants(t, model.Labels(), test.Labels)
	assertEquivalentNameVariants(t, model.TrainingSets(), test.TrainingSets)
}

func expectedModels() ModelTests {
	return ModelTests{
		{
			Name:         "fraud",
			Description:  "fraud model",
			Labels:       []NameVariant{},
			Features:     []NameVariant{},
			TrainingSets: []NameVariant{},
		},
	}
}

func TestListModels(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	models, err := client.ListModels(context.Background())
	if err != nil {
		t.Fatalf("Failed to list models: %v", models)
	}
	expectedModels().Test(t, client, models)
}

func TestGetModels(t *testing.T) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	exp := expectedModels()
	names := make([]string, len(exp))
	for i, e := range exp {
		names[i] = e.Name
	}
	models, err := client.GetModels(context.Background(), names)
	if err != nil {
		t.Fatalf("Failed to get models: %v", names)
	}
	exp.Test(t, client, models)

	model, err := client.GetModel(context.Background(), names[0])
	if err != nil {
		t.Fatalf("Failed to get model: %v", names[0])
	}
	exp[0].Test(t, client, model)

	noModels, err := client.GetModels(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Failed to get no models")
	}
	if len(noModels) != 0 {
		t.Fatalf("Got models when expected none: %+v", noModels)
	}
}
