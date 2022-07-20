from .register import *
from .serving import Client as servClient, LocalClient as servLocalClient

ServingClient = servClient
ResourceClient = Client
ServingLocalClient = servLocalClient
