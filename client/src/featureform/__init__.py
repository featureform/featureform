from .register import *
from .serving import ServingClient
from .resources import DatabricksCredentials, EMRCredentials

ServingClient = ServingClient
ResourceClient = ResourceClient


# Executor Credentials
DatabricksCredentials = DatabricksCredentials
EMRCredentials = EMRCredentials

local = register_local()
register_user("default_user").make_default_owner()