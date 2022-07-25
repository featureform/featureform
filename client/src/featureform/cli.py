# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import click
from featureform import ResourceClient
import featureform.register as register
import grpc
from featureform import ResourceClient
from .list import *
from featureform.proto import metadata_pb2_grpc as ff_grpc
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
@click.argument("resource_type", required=True)
@click.argument("name", required=True)
@click.argument("variant", required=False)
def get(host, cert, insecure, local, resource_type, name, variant):
    """Get resources of a given type.
    """
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")

    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set with --host flag or in env as FEATUREFORM_HOST")

    if insecure:
        rc = ResourceClient(host, False)
    else:
        rc = ResourceClient(host, True, cert)

    rc_get_functions_variant = {
        "feature": rc.get_feature,
        "label": rc.get_label,
        "source": rc.get_source,
        "trainingset": rc.get_training_set,
        "training-set": rc.get_training_set
    }

    rc_get_functions = {
        "user": rc.get_user,
        "model": rc.get_model,
        "entity": rc.get_entity,
        "provider": rc.get_provider
    }

    if resource_type in rc_get_functions_variant:
        rc_get_functions_variant[resource_type](name, variant)
    elif resource_type in rc_get_functions:
        rc_get_functions[resource_type](name)
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
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")

    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set with --host flag or in env as FEATUREFORM_HOST")
                
    if insecure:
        rc = ResourceClient(host, False)
    else:
        rc = ResourceClient(host, True, cert)

    rc_list_functions = {
        "features": rc.list_features,
        "labels": rc.list_labels,
        "sources": rc.list_sources,
        "trainingsets": rc.list_sources,
        "training-sets": rc.list_training_sets,
        "users": rc.list_users,
        "models": rc.list_models,
        "entities": rc.list_entities,
        "providers": rc.list_providers
    }

    if resource_type in rc_list_functions:
        rc_list_functions[resource_type]()
    else:
        raise ValueError("Resource type not found")

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
    for file in files:
        with open(file, "r") as py:
            exec(py.read())

    rc = ResourceClient(host, local, insecure, cert)
    rc.apply()


if __name__ == '__main__':
    cli()
