from .register import *
from .serving import ServingClient
from .resources import DatabricksCredentials, EMRCredentials, AWSCredentials, SparkCredentials

ServingClient = ServingClient
ResourceClient = ResourceClient


# Executor Credentials
DatabricksCredentials = DatabricksCredentials
EMRCredentials = EMRCredentials
SparkCredentials = SparkCredentials

# Cloud Provider Credentials
AWSCredentials = AWSCredentials

local = register_local()
register_user("default_user").make_default_owner()