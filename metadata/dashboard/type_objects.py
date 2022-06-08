from ssl import create_default_context

# USE THIS FILE SINCE IT HAS THE CORRECT ORDER ACCORDING TO THE TABLES KSSHIRAJA MADE
# NOTE: THE ORDER OF VARIABLES IN THE INIT FUNCTION IS CORRECT BUT THE ORDER OF VARIABLES 
# IN THE ARGUMENT (PARENTHESIS) OF THE INIT FUNCTION IS INCORRECT, PLEASE FIX

class FeatureVariantResource:
    def __init__(self, 
        created=None, 
        description="", 
        entity="", 
        name="", 
        owner="", 
        provider="", 
        dataType="", 
        variant="",
        status="",
        location=None,
        source=None,
        trainingSets=None):

        self.created = created
        self.description = description
        self.entity = entity
        self.name = name
        self.owner = owner
        self.provider = provider
        self.dataType = dataType
        self.variant = variant
        self.status = status
        self.location = location
        self.source = source
        self.trainingSets = trainingSets

# Final order
class FeatureResource:
    def __init__(self, 
        allVariants=[],
        type = "",
        defaultVariant="",
        name="",
        variants=None):

        self.name = name
        self.defaultVariant = defaultVariant
        self.type = type
        self.allVariants = allVariants
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
        self.label = label
        self.features = features
        self.status = status

class TrainingSetResource:
    def __init__(self, 
        allVariants=[],
        type = "",
        defaultVariant="",
        name="",
        variants=None):
        self.type = type
        self.defaultVariant = defaultVariant
        self.name = name
        self.variants = variants
        self.allVariants = allVariants

class SourceVariantResource:
    def __init__(self, 
        created=None, 
        description="", 
        name="", 
        sourceType = "",
        owner="", 
        provider="", 
        variant="",
        status="",
        definition="",
        labels=None,
        features=None,
        trainingSets=None):

        self.created = created
        self.description = description
        self.name = name
        self.sourceType = sourceType
        self.owner = owner
        self.provider = provider
        self.variant = variant
        self.status = status
        self.definition = definition
        self.labels = labels
        self.features = features
        self.trainingSets = trainingSets

class SourceResource:
    def __init__(self, 
        allVariants=[],
        type = "",
        defaultVariant="",
        name="",
        variants=None):
        self.type = type
        self.defaultVariant = defaultVariant
        self.name = name
        self.variants = variants
        self.allVariants = allVariants

class LabelVariantResource:
    def __init__(self, 
        created=None, 
        description="",
        entity="",
        name="", 
        dataType = "",
        owner="", 
        provider="", 
        variant="",
        status="",
        source=None,
        trainingSets=None,
        location=None):

        self.created = created
        self.description = description
        self.entity = entity
        self.name = name
        self.owner = owner
        self.provider = provider
        self.dataType = dataType
        self.variant = variant
        self.location = location
        self.source = source
        self.status = status
        self.trainingSets = trainingSets

class LabelResource:
    def __init__(self, 
        type = "",
        defaultVariant="",
        name="",
        variants=None,
        allVariants=[]):
        self.type = type
        self.defaultVariant = defaultVariant
        self.name = name
        self.variants = variants
        self.allVariants = allVariants

class EntityResource:
    def __init__(self, 
    description="", 
    type="", 
    name="", 
    features=None, 
    labels=None, 
    trainingSets=None,
    status=""):
        self.name = name
        self.type = type
        self.description = description
        self.status = status
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets

class UserResource:
    def __init__(self, 
        name="",
        type="",
        features=None,
        labels=None,
        trainingSets=None,
        sources=None,
        status=""):

        self.name = name
        self.type = type
        self.status = status
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets
        self.sources = sources

class ModelResource:
    def __init__(self, 
        name="",
        type="",
        description="",
        features=None,
        labels=None,
        trainingSets=None,
        status=""):

        self.name = name
        self.type = type
        self.description = description
        self.status = status
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets 

class ProviderResource:
    def __init__(self, 
        name="",
        type="",
        description="",
        providerType="",
        software="",
        team="",
        sources=None,
        features=None,
        labels=None,
        trainingSets=None,
        status="",
        serializedConfig=""):

        self.name = name
        self.type = type
        self.description = description
        self.providerType=providerType
        self.software=software
        self.team=team
        self.sources=sources
        self.status = status 
        self.serializedConfig=serializedConfig
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets
