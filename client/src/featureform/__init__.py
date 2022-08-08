from .register import *
from .serving import Client as servClient

ServingClient = servClient
ResourceClient = Client

register_user("featureformer").make_default_owner()