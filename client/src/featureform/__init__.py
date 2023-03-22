from .register import *
from .serving import ServingClient
from .resources import DatabricksCredentials, EMRCredentials, AWSCredentials, GCPCredentials, SparkCredentials

ServingClient = ServingClient
ResourceClient = ResourceClient

def entity(cls):
	name = cls.__name__.lower()
	print(f"Registering entity '{name}' with ff")
	return cls

class Feature:
	def __init__(self, registrar, source_name_variant, columns, variant="default", inference_store: Union[str, OnlineProvider, FileStoreProvider] = ""):
		self.registrar = registrar
		self.source = source_name_variant
		self.entity_column = columns[0]
		self.source_column = columns[1]
		self.variant = variant
		self.inference_store = inference_store


class Label:
	def __init__(self, registrar, source_name_variant, columns, variant="default", inference_store: Union[str, OnlineProvider, FileStoreProvider] = ""):
		self.registrar = registrar
		self.source = source_name_variant
		self.entity_column = columns[0]
		self.source_column = columns[1]
		self.variant = variant
		self.inference_store = inference_store


# Executor Credentials
DatabricksCredentials = DatabricksCredentials
EMRCredentials = EMRCredentials
SparkCredentials = SparkCredentials

# Cloud Provider Credentials
AWSCredentials = AWSCredentials
GCPCredentials = GCPCredentials

local = register_local()
register_user("default_user").make_default_owner()

Feature = Feature
Label = Label