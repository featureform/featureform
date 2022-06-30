# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from curses import meta
import click
import featureform.register as register
import grpc
from .proto import metadata_pb2_grpc as ff_grpc
from .proto import metadata_pb2
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
            if not name:
                print("Please enter a name of a user.")
                return
            searchName = metadata_pb2.Name(name=name)
            try:
                for user in stub.GetUsers(iter([searchName])):
                    print("USER NAME: ", user.name)
                    print("")
                    print("{:<30} {:<35} {:<40}".format('NAME', 'VARIANT', 'TYPE'))
                    for f in user.features:
                        print("{:<30} {:<35} {:<40}".format(
                            f.name, f.variant, "feature"))
                    for l in user.labels:
                        print("{:<30} {:<35} {:<40}".format(
                            l.name, l.variant, "label"))
                    for t in user.trainingsets:
                        print("{:<30} {:<35} {:<40}".format(
                            t.name, t.variant, "training set"))
                    for s in user.sources:
                        print("{:<30} {:<35} {:<40}".format(
                            s.name, s.variant, "source"))
            except grpc._channel._MultiThreadedRendezvous:
                print("User not found.")
        case "feature" | "label":
            if not variant:
                if resource_type == "feature":
                    f = stub.GetFeatures
                if resource_type == "label":
                    f = stub.GetLabels
                searchName = metadata_pb2.Name(name=name)
                for x in f(iter([searchName])):
                    print("NAME: ", x.name)
                    print("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
                    print("VARIANTS:")
                    print("{:<20} {:<25}".format(x.default_variant, 'default'))
                    for v in x.variants:
                        if v != x.default_variant:
                            print("{:<20} {:<35}".format(v, ''))
            else:
                if resource_type == "feature":
                    f = stub.GetFeatureVariants
                if resource_type == "label":
                    f = stub.GetLabelVariants
                searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
                try:
                    for x in f(iter([searchNameVariant])):
                        print("{:<20} {:<15}".format("NAME: ", x.name))
                        print("{:<20} {:<15}".format("VARIANT: ", x.variant))
                        print("{:<20} {:<15}".format("TYPE:", x.type))
                        print("{:<20} {:<15}".format("ENTITY:", x.entity))
                        print("{:<20} {:<15}".format("OWNER:", x.owner))
                        print("{:<20} {:<15}".format("DESCRIPTION:", x.description))
                        print("{:<20} {:<15}".format("PROVIDER:", x.provider))
                        print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
                        print("")
                        print("SOURCE: ")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        print("{:<30} {:<35}".format(x.source.name, x.source.variant))
                        print("")
                        print("TRAINING SETS:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for t in x.trainingsets:
                            print("{:<30} {:<35}".format(t.name, t.variant))
                except grpc._channel._MultiThreadedRendezvous:
                    print(f"{resource_type.capitalize()} variant not found.")
        case "source":
            if not variant:
                searchName = metadata_pb2.Name(name=name)
                try:
                    for x in stub.GetSources(iter([searchName])):
                        print("NAME: ", x.name)
                        print("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
                        print("VARIANTS:")
                        print("{:<20} {:<25}".format(x.default_variant, 'default'))
                        for v in x.variants:
                            if v != x.default_variant:
                                print("{:<20} {:<35}".format(v, ''))
                except grpc._channel._MultiThreadedRendezvous:
                    print(f"{resource_type.capitalize()} not found.")
            else:
                searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
                try:
                    for x in stub.GetSourceVariants(iter([searchNameVariant])):
                        print("{:<20} {:<15}".format("NAME: ", x.name))
                        print("{:<20} {:<15}".format("VARIANT: ", x.variant))
                        print("{:<20} {:<15}".format("OWNER:", x.owner))
                        print("{:<20} {:<15}".format("DESCRIPTION:", x.description))
                        print("{:<20} {:<15}".format("PROVIDER:", x.provider))
                        print("{:<20} {:<15}".format("TABLE:", x.table))
                        print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
                        print("")
                        print("DEFINITION:")
                        print("TRANSFORMATION")
                        print(x.transformation.SQLTransformation.query)
                        print("")
                        print("SOURCES")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for s in x.transformation.SQLTransformation.source:
                            print("{:<30} {:<35}".format(s.name, s.variant))
                        print("")
                        print("PRIMARY DATA")
                        print(x.primaryData.table.name)
                        print("FEATURES:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for t in x.features:
                            print("{:<30} {:<35}".format(t.name, t.variant))
                        print("")
                        print("LABELS:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for t in x.labels:
                            print("{:<30} {:<35}".format(t.name, t.variant))
                        print("")
                        print("TRAINING SETS:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for t in x.trainingsets:
                            print("{:<30} {:<35}".format(t.name, t.variant))
                except grpc._channel._MultiThreadedRendezvous:
                    print(f"{resource_type.capitalize()} variant not found.")
        case "training-set":
            if not variant:
                searchName = metadata_pb2.Name(name=name)
                try:
                    for x in stub.GetTrainingSets(iter([searchName])):
                        print("NAME: ", x.name)
                        print("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
                        print("VARIANTS:")
                        print("{:<20} {:<25}".format(x.default_variant, 'default'))
                        for v in x.variants:
                            if v != x.default_variant:
                                print("{:<20} {:<35}".format(v, ''))
                except grpc._channel._MultiThreadedRendezvous:
                    print(f"{resource_type.capitalize()} not found.")
            else:
                searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
                try:
                    for x in stub.GetTrainingSetVariants(iter([searchNameVariant])):
                        print("{:<20} {:<15}".format("NAME: ", x.name))
                        print("{:<20} {:<15}".format("VARIANT: ", x.variant))
                        print("{:<20} {:<15}".format("OWNER:", x.owner))
                        print("{:<20} {:<15}".format("DESCRIPTION:", x.description))
                        print("{:<20} {:<15}".format("PROVIDER:", x.provider))
                        print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
                        print("")
                        print("LABEL: ")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        print("{:<30} {:<35}".format(x.label.name, x.label.variant))
                        print("")
                        print("FEATURES:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for f in x.features:
                            print("{:<30} {:<35}".format(f.name, f.variant))
                except grpc._channel._MultiThreadedRendezvous:
                    print(f"{resource_type.capitalize()} variant not found.")
        case "provider":
            if variant:
                print("Variant not needed.")
                return
            else:
                searchName = metadata_pb2.Name(name=name)
                try:
                    for x in f(iter([searchName])):
                        print("{:<20} {:<15}".format("NAME: ", x.name))
                        print("{:<20} {:<15}".format("DESCRIPTION: ", x.description))
                        print("{:<20} {:<15}".format("TYPE: ", x.type))
                        print("{:<20} {:<15}".format("SOFTWARE: ", x.software))
                        print("{:<20} {:<15}".format("TEAM: ", x.team))
                        print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
                        print("")
                        print("SOURCES:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for s in x.sources:
                            print("{:<30} {:<35}".format(s.name, s.variant))
                        print("")
                        print("FEATURES:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for f in x.features:
                            print("{:<30} {:<35}".format(f.name, f.variant))
                        print("")
                        print("LABELS:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for l in x.labels:
                            print("{:<30} {:<35}".format(l.name, l.variant))
                        print("")
                        print("TRAINING SETS:")
                        print("{:<30} {:<35}".format("NAME", "VARIANT"))
                        for t in x.trainingsets:
                            print("{:<30} {:<35}".format(t.name, t.variant))
                except grpc._channel._MultiThreadedRendezvous:
                    print(f"{resource_type.capitalize()} not found.")
        case "entity" | "model":
            if resource_type == "feature":
                f = stub.GetEntities
            if resource_type == "label":
                f = stub.GetModels
            if variant:
                print("Variant not needed.")
                return
            else:
                searchName = metadata_pb2.Name(name=name)
                try:
                    for x in f(iter([searchName])):
                        print("{:<20} {:<15}".format("NAME: ", x.name))
                        print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
                        print("VARIANTS:")
                        print("{:<20} {:<25}".format(x.default_variant, 'default'))
                        for v in x.variants:
                            if v != x.default_variant:
                                print("{:<20} {:<35}".format(v, ''))
                except grpc._channel._MultiThreadedRendezvous:
                    print(f"{resource_type.capitalize()} not found.")
        case _:
            print("Resource type not found.")


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
def list(host, cert, insecure, resource_type):
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
        case "users":
            pass
        case "features":
            pass
        case "labels":
            pass
        case "sources":
            pass
        case "training-sets":
            pass
        case "entities":
            pass
        case "providers":
            pass
        case "models":
            pass
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
