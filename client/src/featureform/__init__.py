from .register import *
from .serving import Client as servClient

ServingClient = servClient
ResourceClient = Client

local = register_local()
