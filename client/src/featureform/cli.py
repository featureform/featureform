#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os
import click
import validators
import urllib.request

from .client import Client
from .deploy import (
    DockerDeployment,
)

from .version import get_package_version
from .tls import get_version_hosted

resource_types = [
    "feature",
    "source",
    "training-set",
    "label",
    "entity",
    "provider",
    "model",
    "user",
]

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
SUPPORTED_DEPLOY_TYPES = ["docker"]


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    # fmt: off
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
    # fmt: on
    pass


@cli.command()
@click.option(
    "--host",
    "host",
    required=False,
    help="The host address of the API server to connect to",
)
@click.option(
    "--cert", "cert", required=False, help="Path to self-signed TLS certificate"
)
@click.option("--insecure", is_flag=True, help="Disables TLS verification")
@click.argument("resource_type", required=True)
@click.argument("name", required=True)
@click.argument("variant", required=False)
def get(host, cert, insecure, resource_type, name, variant):
    """Get resources of a given type."""
    host = host or os.getenv("FEATUREFORM_HOST")
    if host is None:
        raise ValueError(
            "Host value must be set with --host flag or in env as FEATUREFORM_HOST"
        )

    client = Client(host=host, insecure=insecure, cert_path=cert)

    resource_get_functions_variant = {
        "feature": client.print_feature,
        "label": client.print_label,
        "source": client.print_source,
        "trainingset": client.print_training_set,
        "training-set": client.print_training_set,
    }

    resource_get_functions = {
        "user": client.get_user,
        "model": client.get_model,
        "entity": client.get_entity,
        "provider": client.get_provider,
    }

    if resource_type in resource_get_functions_variant:
        resource_get_functions_variant[resource_type](
            name=name,
            variant=variant,
        )
    elif resource_type in resource_get_functions:
        resource_get_functions[resource_type](name=name)
    else:
        raise ValueError("Resource type not found")


@cli.command()
@click.option(
    "--host",
    "host",
    required=False,
    help="The host address of the API server to connect to",
)
@click.option(
    "--cert", "cert", required=False, help="Path to self-signed TLS certificate"
)
@click.option("--insecure", is_flag=True, help="Disables TLS verification")
@click.argument("resource_type", required=True)
def list(host, cert, insecure, resource_type):
    host = host or os.getenv("FEATUREFORM_HOST")
    if host is None:
        raise ValueError(
            "Host value must be set with --host flag or in env as FEATUREFORM_HOST"
        )

    client = Client(host=host, insecure=insecure, cert_path=cert)

    resource_list_functions = {
        "features": client.list_features,
        "labels": client.list_labels,
        "sources": client.list_sources,
        "trainingsets": client.list_training_sets,
        "training-sets": client.list_training_sets,
        "users": client.list_users,
        "models": client.list_models,
        "entities": client.list_entities,
        "providers": client.list_providers,
    }

    if resource_type in resource_list_functions:
        resource_list_functions[resource_type]()
    else:
        raise ValueError("Resource type not found")


@cli.command()
def version():
    client_version = get_package_version()
    host = os.getenv("FEATUREFORM_HOST", "")
    cluster_version = ""
    output = f"Client Version: {client_version}"
    try:
        cluster_version = get_version_hosted(host)
    except Exception:
        cluster_version = "Cannot retrieve: Check your FEATUREFORM_HOST value."
    output += f"\nCluster Version: {cluster_version}"

    print(output)


@cli.command()
@click.argument("files", required=True, nargs=-1)
@click.option(
    "--host",
    "host",
    required=False,
    help="The host address of the API server to connect to",
)
@click.option(
    "--cert", "cert", required=False, help="Path to self-signed TLS certificate"
)
@click.option("--insecure", is_flag=True, help="Disables TLS verification")
@click.option(
    "--dry-run", is_flag=True, help="Checks the definitions without applying them"
)
@click.option("--no-wait", is_flag=True, help="Applies the resources asynchronously")
@click.option(
    "--verbose", is_flag=True, help="Prints all errors at the end of an apply"
)
def apply(host, cert, insecure, files, dry_run, no_wait, verbose):
    # The client must be initialized *before* the files are compiled and executed
    # so the default owner can be registered ahead of any resources that require it.
    client = Client(host=host, insecure=insecure, cert_path=cert, dry_run=dry_run)
    for file in files:
        if os.path.isfile(file):
            read_file(file)
        elif validators.url(file):
            read_url(file)
        # In a directory, all files are applied in alphabetical order. Subdirectories are ignored.
        elif os.path.isdir(file):
            read_dir(file)
        else:
            raise ValueError(
                f"Argument must be a path to a file, directory or URL with a valid schema (http:// or https://): {file}"
            )
    asynchronous = no_wait
    client.apply(asynchronous=asynchronous, verbose=verbose)


@cli.command()
@click.option(
    "--query",
    "-q",
    "query",
    required=True,
    help="The phrase to search resources (e.g. 'quick').",
)
@click.option(
    "--host",
    "host",
    required=False,
    help="The host address of the API server to connect to",
)
@click.option(
    "--cert", "cert", required=False, help="Path to self-signed TLS certificate"
)
@click.option("--insecure", is_flag=True, help="Disables TLS verification")
def search(query, host, cert, insecure):
    client = Client(host=host, insecure=insecure, cert_path=cert)
    _ = client.search(query)


@cli.command()
@click.argument(
    "deploy_type",
    required=True,
    default="docker",
    type=click.Choice(SUPPORTED_DEPLOY_TYPES, case_sensitive=False),
)
@click.option(
    "--quickstart", is_flag=True, help="Install Featureform Quickstart as well"
)
@click.option(
    "--include_clickhouse",
    is_flag=True,
    help="Includes ClickHouse in the deployment. Requires quickstart.",
)
def deploy(deploy_type, quickstart, include_clickhouse):
    print(f"Deploying Featureform on {deploy_type.capitalize()}")
    if deploy_type.lower() == "docker":
        deployment = DockerDeployment(quickstart, clickhouse=include_clickhouse)
    else:
        supported_types = ", ".join(SUPPORTED_DEPLOY_TYPES)
        raise ValueError(
            f"Invalid deployment type: Supported types are '{supported_types}'"
        )

    deployment_status = deployment.start()
    return deployment_status


@cli.command()
@click.argument(
    "deploy_type",
    required=True,
    default="docker",
    type=click.Choice(SUPPORTED_DEPLOY_TYPES, case_sensitive=False),
)
def stop(deploy_type):
    print(f"Tearing down Featureform on {deploy_type.capitalize()}")
    if deploy_type.lower() == "docker":
        deployment = DockerDeployment(True, clickhouse=True)
    else:
        supported_types = ", ".join(SUPPORTED_DEPLOY_TYPES)
        raise ValueError(
            f"Invalid deployment type: Supported types are '{supported_types}'"
        )

    deployment_status = deployment.stop()
    return deployment_status


def read_file(file):
    with open(file, "r") as py:
        exec_file(py, file)


def read_url(url):
    try:
        with urllib.request.urlopen(url) as py:
            exec_file(py, url)
    except Exception as e:
        raise ValueError(f"Could not apply the provided URL: {e}: {url}")


def read_dir(directory):
    for root, _, files in os.walk(directory):
        files.sort()
        for file in files:
            read_file(os.path.join(root, file))


def exec_file(file, name):
    code = compile(file.read(), name, "exec")
    # Create a new global namespace for each file to ensure that
    # global variables, such as `ff`, are not undefined in the
    # context of class attribute assignments (e.g. `label = ff.Label()`)
    file_globals = {}
    exec(code, file_globals)


if __name__ == "__main__":
    cli()
