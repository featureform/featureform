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
	default:
		panic("ResourceType not handled")
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

type UserTest struct {
	Name         string
	Features     []NameVariant
	Labels       []NameVariant
	TrainingSets []NameVariant
	Sources      []NameVariant
}

func (test UserTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test UserTest) Test(t *testing.T, client *Client, res interface{}) {
	user := res.(*User)
	assertEqual(t, user.Name(), test.Name)
	assertEquivalentNameVariants(t, user.Features(), test.Features)
	assertEquivalentNameVariants(t, user.Labels(), test.Labels)
	assertEquivalentNameVariants(t, user.TrainingSets(), test.TrainingSets)
	assertEquivalentNameVariants(t, user.Sources(), test.Sources)
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

func TestUser(t *testing.T) {
	testListResources(t, USER, expectedUsers())
	testGetResources(t, USER, expectedUsers())
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

func (test ProviderTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test ProviderTest) Test(t *testing.T, client *Client, res interface{}) {
	provider := res.(*Provider)
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

func expectedProviders() ResourceTests {
	return ResourceTests{
		ProviderTest{
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
		ProviderTest{
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

func TestProvider(t *testing.T) {
	testListResources(t, PROVIDER, expectedProviders())
	testGetResources(t, PROVIDER, expectedProviders())
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

func (test EntityTest) Test(t *testing.T, client *Client, res interface{}) {
	t.Logf("Testing entity: %s", test.Name)
	entity := res.(*Entity)
	assertEqual(t, entity.Name(), test.Name)
	assertEqual(t, entity.Description(), test.Description)
	assertEquivalentNameVariants(t, entity.Features(), test.Features)
	assertEquivalentNameVariants(t, entity.Labels(), test.Labels)
	assertEquivalentNameVariants(t, entity.TrainingSets(), test.TrainingSets)
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

func (test SourceVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test SourceVariantTest) Test(t *testing.T, client *Client, res interface{}) {
	t.Logf("Testing source: %s %s", test.Name, test.Variant)
	source := res.(*SourceVariant)
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

func expectedSources() ResourceTests {
	return ResourceTests{
		ParentResourceTest{
			Name:     "mockSource",
			Variants: []string{"var", "var2"},
			Default:  "var",
		},
	}
}

func expectedSourceVariants() ResourceTests {
	return ResourceTests{
		SourceVariantTest{
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
		SourceVariantTest{
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

func TestSource(t *testing.T) {
	testListResources(t, SOURCE, expectedSources())
	testGetResources(t, SOURCE, expectedSources())
	testGetResources(t, SOURCE_VARIANT, expectedSourceVariants())
}

func expectedFeatures() ResourceTests {
	return ResourceTests{
		ParentResourceTest{
			Name:     "feature",
			Variants: []string{"variant", "variant2"},
			Default:  "variant",
		},
		ParentResourceTest{
			Name:     "feature2",
			Variants: []string{"variant"},
			Default:  "variant",
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
}

func (test FeatureVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test FeatureVariantTest) Test(t *testing.T, client *Client, res interface{}) {
	t.Logf("Testing feature: %s %s", test.Name, test.Variant)
	feature := res.(*FeatureVariant)
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
		},
	}
}

func TestFeature(t *testing.T) {
	testListResources(t, FEATURE, expectedFeatures())
	testGetResources(t, FEATURE, expectedFeatures())
	testGetResources(t, FEATURE_VARIANT, expectedFeatureVariants())
}

func expectedLabels() ResourceTests {
	return ResourceTests{
		ParentResourceTest{
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
}

func (test LabelVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test LabelVariantTest) Test(t *testing.T, client *Client, res interface{}) {
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
	assertEquivalentNameVariants(t, label.TrainingSets(), test.TrainingSets)
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
		},
	}
}

func TestLabel(t *testing.T) {
	testListResources(t, LABEL, expectedLabels())
	testGetResources(t, LABEL, expectedLabels())
	testGetResources(t, LABEL_VARIANT, expectedLabelVariants())
}

func expectedTrainingSets() ResourceTests {
	return ResourceTests{
		ParentResourceTest{
			Name:     "training-set",
			Variants: []string{"variant", "variant2"},
			Default:  "variant",
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
}

func (test TrainingSetVariantTest) NameVariant() NameVariant {
	return NameVariant{test.Name, test.Variant}
}

func (test TrainingSetVariantTest) Test(t *testing.T, client *Client, resource interface{}) {
	t.Logf("Testing trainingSet: %s %s", test.Name, test.Variant)
	trainingSet := resource.(*TrainingSetVariant)
	assertEqual(t, trainingSet.Name(), test.Name)
	assertEqual(t, trainingSet.Variant(), test.Variant)
	assertEqual(t, trainingSet.Description(), test.Description)
	assertEqual(t, trainingSet.Owner(), test.Owner)
	assertEqual(t, trainingSet.Provider(), test.Provider)
	assertEqual(t, trainingSet.Label(), test.Label)
	assertEquivalentNameVariants(t, trainingSet.Features(), test.Features)
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
			Owner: "Other",
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
			Owner: "Featureform",
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
}

func (test ModelTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test ModelTest) Test(t *testing.T, client *Client, resource interface{}) {
	t.Logf("Testing model: %s", test.Name)
	model := resource.(*Model)
	assertEqual(t, model.Name(), test.Name)
	assertEqual(t, model.Description(), test.Description)
	assertEquivalentNameVariants(t, model.Features(), test.Features)
	assertEquivalentNameVariants(t, model.Labels(), test.Labels)
	assertEquivalentNameVariants(t, model.TrainingSets(), test.TrainingSets)
}

func expectedModels() ResourceTests {
	return ResourceTests{
		ModelTest{
			Name:         "fraud",
			Description:  "fraud model",
			Labels:       []NameVariant{},
			Features:     []NameVariant{},
			TrainingSets: []NameVariant{},
		},
	}
}

func TestModel(t *testing.T) {
	testListResources(t, MODEL, expectedModels())
	testGetResources(t, MODEL, expectedModels())
}

type ParentResourceTest struct {
	Name     string
	Variants []string
	Default  string
}

func (test ParentResourceTest) NameVariant() NameVariant {
	return NameVariant{Name: test.Name}
}

func (test ParentResourceTest) Test(t *testing.T, client *Client, resource interface{}) {
	t.Logf("Testing ParentResource: %s", test.Name)
	type ParentResource interface {
		Name() string
		Variants() []string
		DefaultVariant() string
	}
	parentRes := resource.(ParentResource)
	assertEqual(t, parentRes.Name(), test.Name)
	assertEqual(t, parentRes.Variants(), test.Variants)
	assertEqual(t, parentRes.DefaultVariant(), test.Default)
}

type ResourceTest interface {
	NameVariant() NameVariant
	Test(t *testing.T, client *Client, resources interface{})
}

type ResourceTests []ResourceTest

func (tests ResourceTests) NameVariants() NameVariants {
	nameVars := make(NameVariants, len(tests))
	for i, test := range tests {
		nameVars[i] = test.NameVariant()
	}
	return nameVars
}

func (tests ResourceTests) Test(t *testing.T, client *Client, resources interface{}) {
	testMap := make(map[NameVariant]ResourceTest)
	for _, test := range tests {
		testMap[test.NameVariant()] = test
	}
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
		test.Test(t, client, res)
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
	client := ctx.Create(t)
	defer ctx.Destroy()
	names := tests.NameVariants()
	resources, err := getAll(client, typ, names)
	if err != nil {
		t.Fatalf("Failed to get resources: %v", names)
	}
	tests.Test(t, client, resources)

	resource, err := get(client, typ, names[0])
	if err != nil {
		t.Fatalf("Failed to get resource: %v", names[0])
	}
	tests[0].Test(t, client, resource)

	noResources, err := getAll(client, typ, NameVariants{})
	if err != nil {
		t.Fatalf("Failed to get no resources")
	}
	if reflect.ValueOf(noResources).Len() != 0 {
		t.Fatalf("Got resources when expected none: %+v", noResources)
	}
}

func testListResources(t *testing.T, typ ResourceType, tests ResourceTests) {
	ctx := testContext{
		Defs: filledResourceDefs(),
	}
	client := ctx.Create(t)
	defer ctx.Destroy()
	resources, err := list(client, typ)
	if err != nil {
		t.Fatalf("Failed to list resources: %v", resources)
	}
	tests.Test(t, client, resources)
}
