# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from curses import meta
import click
import featureform.register as register
import grpc
from featureform import ResourceClient
from .list import *
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
    "model",
    "user"
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
@click.option("--local",
              is_flag=True,
              help="Enables local mode")
@click.argument("resource_type", required=True, help="Type of resource to be retrieved. Can be feature, user, source, provider, entity, model, label, or training-set.")
@click.argument("name", required=True, help="Name of resource to be retrieved.")
@click.argument("variant", required=False, help="Variant of resource to be retrieved, optional.")
def get(host, cert, insecure, local, resource_type, name, variant):
    """Get information about a resource or resource variant of a given type. 
    General information about a resource can be retrieved without the variant paraameter, 
    but to get information on a specific variant, variant must be given.

    :param host: Host address of the API server
    :type host: str
    :param cert: Path to self-signed TLS certificate
    :type cert: str, optional (not given if --insecure is used)
    :param insecure: Disables TLS verification
    :type insecure: str, optional (not given if --cert is used)
    :param local: Enables local mode
    :type local: str, optional
    :param resource_type: Type of resource to be retrieved
    :type resource_type: str, can be "feature", "user", "source", "provider", "entity", "model", "label", or "training-set"
    :param name: Name of resource to be retrieved.
    :type name: str
    :param variant: Variant of resource to be retrieved
    :type variant: str, optional (only given if information on a particular variant is needed)
    :raises ValueError: Cannot be local and have a host
    :raises ValueError: Host value must be set with --host flag or in env as FEATUREFORM_HOST
    :raises ValueError: Variant not needed
    :raises ValueError: Resource type not found
    """
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")

    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set with --host flag or in env as FEATUREFORM_HOST")

    if local:
        register.state().create_all_local()
    else:
        channel = tls_check(host, cert, insecure)
        stub = ff_grpc.ApiStub(channel)
        register.state().create_all(stub)

    rc = ResourceClient(host)

    if resource_type == "feature":
        if not variant:
            rc.get_feature(name)
        else:
            rc.get_feature(name, variant)
    elif resource_type == "label":
        if not variant:
            rc.get_label(name)
        else:
            rc.get_label(name, variant)
    elif resource_type == "source":
        if not variant:
            rc.get_source(name)
        else:
            rc.get_source(name, variant)
    elif resource_type == "trainingset" or resource_type == "training-set":
        if not variant:
            rc.get_training_set(name)
        else:
            rc.get_training_set(name, variant)
    elif resource_type == "user":
        if variant:
            raise ValueError("Variant not needed")
        rc.get_user(name)
    elif resource_type == "model":
        if variant:
            raise ValueError("Variant not needed")
        rc.get_model(name)
    elif resource_type == "entity":
        if variant:
            raise ValueError("Variant not needed")
        rc.get_entity(name)
    elif resource_type == "provider":
        if variant:
            raise ValueError("Variant not needed")
        rc.get_provider(name)
    else:
        raise ValueError("Resource type not found")


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
@click.option("--local",
              is_flag=True,
              help="Enable local mode")
@click.argument("resource_type", required=True)
def list(host, cert, insecure, local, resource_type):
    """List all resources of a given resource type.

    :param host: Host address of the API server
    :type host: str
    :param cert: Path to self-signed TLS certificate
    :type cert: str, optional (not given if --insecure is used)
    :param insecure: Disables TLS verification
    :type insecure: str, optional (not given if --cert is used)
    :param local: Enables local mode
    :type local: str, optional
    :param resource_type: Type of resource to be retrieved
    :type resource_type: str, can be "feature", "user", "source", "provider", "entity", "model", "label", or "training-set"
    :raises ValueError: Cannot be local and have a host
    :raises ValueError: Host value must be set with --host flag or in env as FEATUREFORM_HOST
    :raises ValueError: Resource type not found
    """
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")

    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set with --host flag or in env as FEATUREFORM_HOST")

    if local:
        register.state().create_all_local()
    else:
        channel = tls_check(host, cert, insecure)
        stub = ff_grpc.ApiStub(channel)
        register.state().create_all(stub)

    rc = ResourceClient(host)

    if resource_type == "features":
        rc.list_features()
    elif resource_type == "labels":
        rc.list_labels()
    elif resource_type == "sources":
        rc.list_sources()
    elif resource_type == "trainingsets" or resource_type == "training-sets":
        rc.list_training_sets()
    elif resource_type == "users":
        rc.list_users()
    elif resource_type == "models":
        rc.list_models()
    elif resource_type == "entities":
        rc.list_entities()
    elif resource_type == "providers":
        rc.list_providers()
    else:
        raise ValueError("Resource type not found")

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
def apply(host, cert, insecure, local, files):
    """Submit resource definitions in FILES to the Featureform instance for logging and materialization.

    :param host: Host address of the API server
    :type host: str
    :param cert: Path to self-signed TLS certificate
    :type cert: str, optional (not given if --insecure is used)
    :param insecure: Disables TLS verification
    :type insecure: str, optional (not given if --cert is used)
    :param local: Enables local mode
    :type local: str, optional
    :param files: Path to file with resource definitions.
    :type files: str
    :raises ValueError: Cannot be local and have a host
    :raises ValueError: Host value must be set with --host flag or in env as FEATUREFORM_HOST
    """
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")

    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set with --host flag or in env as FEATUREFORM_HOST")

    for file in files:
        with open(file, "r") as py:
            exec(py.read())

    if local:
        register.state().create_all_local()
    else:
        channel = tls_check(host, cert, insecure)
        stub = ff_grpc.ApiStub(channel)
        register.state().create_all(stub)


def tls_check(host, cert, insecure):
    if insecure:
        channel = grpc.insecure_channel(
            host, options=(('grpc.enable_http_proxy', 0),))
    elif cert != None or os.getenv('FEATUREFORM_CERT') != None:
        if os.getenv('FEATUREFORM_CERT') != None and cert == None:
            cert = os.getenv('FEATUREFORM_CERT')
        with open(cert, 'rb') as f:
            credentials = grpc.ssl_channel_credentials(f.read())
        channel = grpc.secure_channel(host, credentials)
    else:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(host, credentials)
    return channel


if __name__ == '__main__':
    cli()
