# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import click
import featureform.register as register
import grpc
from .proto import metadata_pb2_grpc as ff_grpc

resource_types = [
    "feature",
    "source",
    "training-set",
    "label",
    "entity",
    "provider",
    "transformation",
]

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    \b
    ______         _                   __                     
    |  ___|       | |                 / _|                    
    | |_ ___  __ _| |_ _   _ _ __ ___| |_ ___  _ __ _ __ ___  
    |  _/ _ \/ _` | __| | | | '__/ _ \  _/ _ \| '__| '_ ` _ \ 
    | ||  __/ (_| | |_| |_| | | |  __/ || (_) | |  | | | | | |
    \_| \___|\__,_|\__|\__,_|_|  \___|_| \___/|_|  |_| |_| |_|

    Interact with Featureform's Feature Store via the official command line interface.
    """
    pass


# @cli.command()
# @click.argument("resource_type")
# def list(resource_type):
#     """list resources of a given type.
#     """
#     pass
# 
# 
# @cli.command()
# @click.argument("resource_type",
#                 type=click.Choice(resource_types, case_sensitive=False))
# @click.argument("resources", nargs=-1, required=True)
# def get(resource_type, resoruces):
#     """get resources of a given type.
#     """
#     pass
# 
# 
# @cli.command()
# @click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
# def plan(files):
#     """print out resources that would be changed by applying these files.
#     """
#     pass


@cli.command()
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("--host",
              "host",
              required=True,
              help="The host address of the API server to connect to")
@click.option("--insecure",
              is_flag=True,
              help="Disables TLS verification")
def apply(host, insecure, files):
    """apply changes to featureform
    """
    if insecure:
        channel = grpc.insecure_channel(host, options=(('grpc.enable_http_proxy', 0),))
    else:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(host, credentials)
    stub = ff_grpc.ApiStub(channel)
    for file in files:
        with open(file, "r") as py:
            exec(py.read())
    print(register.state().sorted_list())
    register.state().create_all(stub)


if __name__ == '__main__':
    cli()
