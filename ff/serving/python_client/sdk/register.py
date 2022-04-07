
files_to_register = []
entities_to_register = []
features_to_register = []
labels_to_register = []
training_set_to_register = []

class File:
    def __init__(self, path = str, name = str, index = str, storage = str):
        self.path = path
        self.name = name
        self.index = index
        self.storage = storage

def register_file(path, name = "", index = "", storage = ""):
    newFile = File(path, name, index, storage)
    files_to_register.append(newFile)

class Entity:
    def __init__(self, key = str):
        self.key = key

def register_entity(name = ""):
    entity = Entity(name)
    entities_to_register.append(entity)

class Feature:
    def __init__(self, name : str, description : str, src : str, column : str, entity : str, executor : str, cast :str, frequency: str):
        self.name = name
        self.description = description
        self.src = src
        self.column = column
        self.entity = entity
        self.executor = executor
        self.cast = cast
        self.frequency = frequency

def register_feature_from_source(name = "", description = "", src = "", column = "", entity = "", executor = "", cast = "", frequency = ""):
    feature = Feature(name, description, src, column, entity, executor, cast, frequency)
    features_to_register.append(feature)
    return feature

class Label:
    def __init__(self : str, name : str, description : str, src : str, column : str, entities : str, cast : str, frequency : str):
        self.name = name
        self.description = description
        self.src = src
        self.column = column
        self.entities = entities
        self.cast = cast
        self.frequency = frequency

def register_label_from_source(name = "", description = "", src = "", column = "", entities = "", cast = "", frequency = ""):
    label = Label(name, description, src, column, entities, cast, frequency)
    labels_to_register.append(label)

class TrainingSet:
    def __init__(self, name, label, features, sampling):
        self.name = name
        self.label = label
        self.features = features
        self.sampling = sampling

def register_training_set(name = "", label = "", features = "", sampling = ""):
    training_set = TrainingSet(name, label, features, sampling)
    training_set_to_register.append(training_set)



register_file("s3://featureform-demo/transactions.csv", name="Transactions", index="id", storage="demo-s3")
register_entity("user")
register_feature_from_source(
    name="user_2fa",
    description = "If user has 2fa",
    src="Users",
    column = "2fa",
    entity="user",
    executor="demo-spark",
    cast = bool,
    frequency = "1 hour",
)

register_label_from_source(
    name="is_fraud",
    description="if a transaction is fraud",
    src = "Transactions",
    column = "fraud",
    entities = {
        "user": "user",
    },
    cast = bool,
    frequency = "1hr",
)

register_training_set(
    name="fraud_training_set",
    label="is_fraud",
    features=[
        ("user_transaction_count", "7d"),
        ("number_of_fraud", "90d"),
        ("amt_spent", "30d"),
        "avg_transaction_amt",
        "user_account_age",
        "user_credit_score",
        "user_2fa",
    ],
    sampling="",
)

