# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from curses import meta
import click
import featureform.register as register
import grpc
from .proto import metadata_pb2_grpc as ff_grpc
from .get import *
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


@cli.command()
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
@click.argument("resource_type", required=True)
@click.argument("name", required=True)
@click.argument("variant", required=False)
def get(host, cert, insecure, resource_type, name, variant):
    """list resources of a given type.
    """
    env_cert_path = os.getenv('FEATUREFORM_CERT')
    if host is None:
        env_host = os.getenv('FEATUREFORM_HOST')
        if env_host is None:
            raise ValueError(
                "Host value must be set in env or with --host flag")
        host = env_host
    if insecure:
        channel = grpc.insecure_channel(
            host, options=(('grpc.enable_http_proxy', 0),))
    elif cert is not None or env_cert_path is not None:
        if env_cert_path is not None and cert is None:
            cert = env_cert_path
        with open(cert, 'rb') as f:
            credentials = grpc.ssl_channel_credentials(f.read())
        channel = grpc.secure_channel(host, credentials)
    else:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(host, credentials)
    stub = ff_grpc.ApiStub(channel)

    match resource_type:
        case "user":
            GetUser(stub, name)
        case "feature":
            if not variant:
                GetFeature(stub, name)
            else:
                GetFeatureVariant(stub, name, variant)
        case "label":
            if not variant:
                GetLabel(stub, name)
            else:
                GetLabelVariant(stub, name, variant)
        case "source":
            if not variant:
                GetSource(stub, name)
            else:
                GetSourceVariant(stub, name, variant)
        case "training-set" | "trainingset":
            if not variant:
                GetTrainingSet(stub, name)
            else:
                GetTrainingSetVariant(stub, name, variant)
        case "provider":
            if variant:
                print("Variant not needed.")
                return
            else:
                GetProvider(stub, name)
        case "entity":
            if variant:
                print("Variant not needed.")
                return
            else:
                GetEntity(stub, name)
        case "model":
            if variant:
                print("Variant not needed.")
                return
            else:
                GetModel(stub, name)
        case _:
            print("Resource type not found.")


# @cli.command()
# @click.option("--host",
#               "host",
#               required=False,
#               help="The host address of the API server to connect to")
# @click.option("--cert",
#               "cert",
#               required=False,
#               help="Path to self-signed TLS certificate")
# @click.option("--insecure",
#               is_flag=True,
#               help="Disables TLS verification")
# @click.argument("resource_type", required=True)
# def list(host, cert, insecure, resource_type):
#     """list resources of a given type.
#     """
#     env_cert_path = os.getenv('FEATUREFORM_CERT')
#     if host is None:
#         env_host = os.getenv('FEATUREFORM_HOST')
#         if env_host is None:
#             raise ValueError(
#                 "Host value must be set in env or with --host flag")
#         host = env_host
#     if insecure:
#         channel = grpc.insecure_channel(
#             host, options=(('grpc.enable_http_proxy', 0),))
#     elif cert is not None or env_cert_path is not None:
#         if env_cert_path is not None and cert is None:
#             cert = env_cert_path
#         with open(cert, 'rb') as f:
#             credentials = grpc.ssl_channel_credentials(f.read())
#         channel = grpc.secure_channel(host, credentials)
#     else:
#         credentials = grpc.ssl_channel_credentials()
#         channel = grpc.secure_channel(host, credentials)
#     stub = ff_grpc.ApiStub(channel)

#     match resource_type:
#         case "users":
#             pass
#         case "features":
#             pass
#         case "labels":
#             pass
#         case "sources":
#             pass
#         case "training-sets":
#             pass
#         case "entities":
#             pass
#         case "providers":
#             pass
#         case "models":
#             pass
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
def apply(host, cert, insecure, files):
    """apply changes to featureform
    """
    env_cert_path = os.getenv('FEATUREFORM_CERT')
    if host is None:
        env_host = os.getenv('FEATUREFORM_HOST')
        if env_host is None:
            raise ValueError(
                "Host value must be set in env or with --host flag")
        host = env_host
    if insecure:
        channel = grpc.insecure_channel(
            host, options=(('grpc.enable_http_proxy', 0),))
    elif cert is not None or env_cert_path is not None:
        if env_cert_path is not None and cert is None:
            cert = env_cert_path
        with open(cert, 'rb') as f:
            credentials = grpc.ssl_channel_credentials(f.read())
        channel = grpc.secure_channel(host, credentials)
    else:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(host, credentials)
    stub = ff_grpc.ApiStub(channel)
    for file in files:
        with open(file, "r") as py:
            exec(py.read())
    register.state().create_all(stub)


if __name__ == '__main__':
    cli()
