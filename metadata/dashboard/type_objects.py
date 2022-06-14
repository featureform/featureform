from ssl import create_default_context


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
        source=None):

        self.dictionary = {
            "created": created,
            "description": description,
            "entity": entity,
            "name": name,
            "owner": provider,
            "dataType": dataType,
            "variant": variant,
            "status": status,
            "location":location,
            "source":source
        }

class FeatureResource:
    def __init__(self, 
        name="",
        defaultVariant="",
        type = "",
        variants=None,
        allVariants=[]):

        self.dictionary = {
            "name": name,
            "defaultVariant": defaultVariant,
            "type": type,
            "variants": variants,
            "allVariants": allVariants
        }

        self.allVariants = allVariants
        self.type = type
        self.defaultVariant = defaultVariant
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
        label=None,
        status="",
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
        type = "",
        defaultVariant="",
        name="",
        variants=None,
        allVariants=[]):

        self.allVariants = allVariants
        self.type = type
        self.defaultVariant = defaultVariant
        self.name = name
        self.variants = variants

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
        type = "",
        defaultVariant="",
        name="",
        variants=None,
        allVariants=[]):
        self.allVariants = allVariants
        self.type = type
        self.defaultVariant = defaultVariant
        self.name = name
        self.variants = variants

class LabelVariantResource:
    def __init__(self, 
        created=None, 
        description="",
        entity="",
        name="",
        owner="", 
        provider="",
        dataType = "", 
        variant="",
        location=None,
        status="",
        trainingSets=None):


        self.created = created
        self.description = description
        self.entity = entity
        self.dataType = dataType
        self.name = name
        self.owner = owner
        self.provider = provider
        self.variant = variant
        self.status = status
        self.location = location
        self.trainingSets = trainingSets

class LabelResource:
    def __init__(self, 
        type = "",
        defaultVariant="",
        name="",
        variants=None,
        allVariants=[]):
        self.allVariants = allVariants
        self.type = type
        self.defaultVariant = defaultVariant
        self.name = name
        self.variants = variants

class EntityResource:
    def __init__(self,
        name="", 
        type="",
        description="",
        status="",
        features=None,
        labels=None,
        trainingSets=None):

        self.description = description
        self.type = type
        self.name = name
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets
        self.status = status

class UserResource:
    def __init__(self, 
        name="",
        type="",
        status="",
        features=None,
        labels=None,
        trainingSets=None,
        sources=None):

        self.name = name
        self.type = type
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets
        self.sources = sources
        self.status = status 

class ModelResource:
    def __init__(self, 
        name="",
        type="",
        description="",
        status="",
        features=None,
        labels=None,
        trainingSets=None):

        self.name = name
        self.type = type
        self.description = description
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets
        self.status = status 

class ProviderResource:
    def __init__(self, 
        name="",
        type="",
        description="",
        providerType="",
        software="",
        team="",
        sources=None,
        status="",
        serializedConfig="",
        features=None,
        labels=None,
        trainingSets=None):

        self.name = name
        self.type = type
        self.description = description
        self.providerType=providerType
        self.software=software
        self.team=team
        self.sources=sources
        self.features = features
        self.labels = labels
        self.trainingSets = trainingSets
        self.status = status
        self.serializedConfig=serializedConfig
