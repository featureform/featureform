from .register import *
from .serving import ServingClient

ServingClient = ServingClient
ResourceClient = ResourceClient

local = register_local()
register_user("default_user").make_default_owner()