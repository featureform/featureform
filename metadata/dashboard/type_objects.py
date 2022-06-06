from ssl import create_default_context


class FeatureVariantResource:
    def __init__(self, 
        created=None, 
        description="", 
        entity="", 
        name="", 
        owner="", 
        provider="", 
        datatype="", 
        variant="",
        status="",
        location=None,
        source=None,
        trainingsets=None):

        self.created = created
        self.description = description
        self.entity = entity
        self.name = name
        self.owner = owner
        self.provider = provider
        self.datatype = datatype
        self.variant = variant
        self.status = status
        self.location = location
        self.source = source
        self.trainingsets = trainingsets

class FeatureResource:
    def __init__(self, 
        allvariants=[],
        type = "",
        defaultvariant="",
        name="",
        variants=None):
        self.allvariants = allvariants
        self.type = type
        self.defaultvariant = defaultvariant
        self.name = name
        self.variants = variants

class TrainingSetVariantResource:
    def __init__(self, 
        created=None, 
        description="", 
        name="", 
        owner="", 
        provider="", 
        variant="",
        status="",
        label=None,
        features=None):

        self.created = created
        self.description = description
        self.name = name
        self.owner = owner
        self.provider = provider
        self.variant = variant
        self.status = status
        self.label = label
        self.features = features

class TrainingSetResource:
    def __init__(self, 
        allvariants=[],
        type = "",
        defaultvariant="",
        name="",
        variants=None):
        self.allvariants = allvariants
        self.type = type
        self.defaultvariant = defaultvariant
        self.name = name
        self.variants = variants

class SourceVariantResource:
    def __init__(self, 
        created=None, 
        description="", 
        name="", 
        sourcetype = "",
        owner="", 
        provider="", 
        variant="",
        status="",
        definition="",
        labels=None,
        features=None,
        trainingsets=None):

        self.created = created
        self.description = description
        self.name = name
        self.sourcetype = sourcetype
        self.owner = owner
        self.provider = provider
        self.variant = variant
        self.status = status
        self.definition = definition
        self.labels = labels
        self.features = features
        self.trainingsets = trainingsets

class SourceResource:
    def __init__(self, 
        allvariants=[],
        type = "",
        defaultvariant="",
        name="",
        variants=None):
        self.allvariants = allvariants
        self.type = type
        self.defaultvariant = defaultvariant
        self.name = name
        self.variants = variants

class LabelVariantResource:
    def __init__(self, 
        created=None, 
        description="",
        entity="",
        name="", 
        datatype = "",
        owner="", 
        provider="", 
        variant="",
        status="",
        source=None,
        trainingsets=None,
        location=None):

        self.created = created
        self.description = description
        self.entity = entity
        self.datatype = datatype
        self.name = name
        self.owner = owner
        self.provider = provider
        self.variant = variant
        self.status = status
        self.source = source
        self.location = location
        self.trainingsets = trainingsets

class LabelResource:
    def __init__(self, 
        allvariants=[],
        type = "",
        defaultvariant="",
        name="",
        variants=None):
        self.allvariants = allvariants
        self.type = type
        self.defaultvariant = defaultvariant
        self.name = name
        self.variants = variants

class EntityResource:
    def __init__(self, 
        description="",
        type="",
        name="", 
        features=None,
        labels=None
        trainingsets=None,
        status=""):

        self.created = created
        self.description = description
        self.entity = entity
        self.datatype = datatype
        self.name = name
        self.owner = owner
        self.provider = provider
        self.variant = variant
        self.status = status
        self.source = source
        self.location = location
        self.trainingsets = trainingsets


# type EntityResource struct {
# 	Name         string                                  `json:"name"`
# 	Type         string                                  `json:"type"`
# 	Description  string                                  `json:"description"`
# 	Features     map[string][]FeatureVariantResource     `json:"features"`
# 	Labels       map[string][]LabelVariantResource       `json:"labels"`
# 	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
# 	Status       string                                  `json:"status"`
# }

# type UserResource struct {
# 	Name         string                                  `json:"name"`
# 	Type         string                                  `json:"type"`
# 	Features     map[string][]FeatureVariantResource     `json:"features"`
# 	Labels       map[string][]LabelVariantResource       `json:"labels"`
# 	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
# 	Sources      map[string][]SourceVariantResource      `json:"sources"`
# 	Status       string                                  `json:"status"`
# }

# type ModelResource struct {
# 	Name         string                                  `json:"name"`
# 	Type         string                                  `json:"type"`
# 	Description  string                                  `json:"description"`
# 	Features     map[string][]FeatureVariantResource     `json:"features"`
# 	Labels       map[string][]LabelVariantResource       `json:"labels"`
# 	TrainingSets map[string][]TrainingSetVariantResource `json:"training-sets"`
# 	Status       string                                  `json:"status"`
# }

# type ProviderResource struct {
# 	Name             string                                  `json:"name"`
# 	Type             string                                  `json:"type"`
# 	Description      string                                  `json:"description"`
# 	ProviderType     string                                  `json:"provider-type"`
# 	Software         string                                  `json:"software"`
# 	Team             string                                  `json:"team"`
# 	Sources          map[string][]SourceVariantResource      `json:"sources"`
# 	Features         map[string][]FeatureVariantResource     `json:"features"`
# 	Labels           map[string][]LabelVariantResource       `json:"labels"`
# 	TrainingSets     map[string][]TrainingSetVariantResource `json:"training-sets"`
# 	Status           string                                  `json:"status"`
# 	SerializedConfig string                                  `json:"serialized-config"`
# }