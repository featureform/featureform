from .register import *
from .serving import ServingClient
from .resources import DatabricksCredentials, EMRCredentials, AWSCredentials, GCPCredentials

ServingClient = ServingClient
ResourceClient = ResourceClient


# Executor Credentials
DatabricksCredentials = DatabricksCredentials
EMRCredentials = EMRCredentials

# Cloud Provider Credentials
AWSCredentials = AWSCredentials
GCPCredentials = GCPCredentials

local = register_local()
register_user("default_user").make_default_owner()