# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import click
import featureform.register as register
import grpc
from .proto import metadata_pb2_grpc as ff_grpc
import os

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
              required=False,
              help="The host address of the API server to connect to")
@click.option("--cert",
              "cert",
              required=False,
              help="Path to self-signed TLS certificate")
@click.option("--insecure",
              is_flag=True,
              help="Disables TLS verification")
@click.option("--local",
              is_flag=True,
              help="Enable local mode")

def TlsChecker(host, cert, insecure):
    if insecure:
        channel = grpc.insecure_channel(
            host, options=(('grpc.enable_http_proxy', 0),))
    elif cert != None or os.getenv('FEATUREFORM_HOST') != None:
        if os.getenv('FEATUREFORM_CERT') != None and cert == None:
            cert = os.getenv('FEATUREFORM_CERT')
        with open(cert, 'rb') as f:
            credentials = grpc.ssl_channel_credentials(f.read())
        channel = grpc.secure_channel(host, credentials)
    else:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(host, credentials)
    return channel


def apply(host, cert, insecure, local, files):
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")
    
    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set in env or with --host flag")

    channel = TlsChecker(host, cert, insecure)

    for file in files:
        with open(file, "r") as py:
            exec(py.read())
    
    if local:
        register.state().create_all_local()
    else:
        stub = ff_grpc.ApiStub(channel)
        register.state().create_all(stub)


if __name__ == '__main__':
    cli()
