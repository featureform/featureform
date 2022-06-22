# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from .resources import ResourceState, Provider, RedisConfig, PostgresConfig, SnowflakeConfig, RedshiftConfig, User, Location, Source, \
    PrimaryData, SQLTable, SQLTransformation, Entity, Feature, Label, ResourceColumnMapping, TrainingSet
from .register import Registrar
from typing import Tuple, Callable, TypedDict, List, Union
from typeguard import typechecked, check_type
import grpc
import os
from .proto import metadata_pb2_grpc as ff_grpc
from .proto import metadata_pb2

NameVariant = Tuple[str, str]



class GetResourceWrapper:

    def __init__(self, stub, resource):
        self.__stub = stub
        self.__resource = resource

    def get(self):
        return self.__resource


class GetUser(GetResourceWrapper):

    def __init__(self, stub, user):
        super().__init__(stub, user)


class GetProvider(GetResourceWrapper):

    def __init__(self, stub, provider):
        super().__init__(stub, provider)


class GetOfflineProvider(GetProvider):

    def __init__(self, stub, provider):
        super().__init__(stub, provider)


class GetOnlineProvider(GetProvider):

    def __init__(self, stub, provider):
        super().__init__(stub, provider)


class GetSource(GetResourceWrapper):

    def __init__(self, stub, source):
        super().__init__(stub, source)


class GetEntity(GetResourceWrapper):

    def __init__(self, stub, entity):
        super().__init__(stub, entity)


class GetFeature(GetResourceWrapper):

    def __init__(self, stub, feature):
        super().__init__(stub, feature)


class GetLabel(GetResourceWrapper):

    def __init__(self, stub, label):
        super().__init__(stub, label)


class GetTrainingSet(GetResourceWrapper):

    def __init__(self, stub, training_set):
        super().__init__(stub, training_set)


class GetClient(Registrar):
    def __init__(self, host, tls_verify=True, cert_path=None):
        super().__init__()
        env_cert_path = os.getenv('FEATUREFORM_CERT')
        if tls_verify:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(host, credentials)
        elif cert_path is not None or env_cert_path is not None:
            if env_cert_path is not None and cert_path is None:
                cert_path = env_cert_path
            with open(cert_path, 'rb') as f:
                credentials = grpc.ssl_channel_credentials(f.read())
            channel = grpc.secure_channel(host, credentials)
        else:
            channel = grpc.insecure_channel(host, options=(('grpc.enable_http_proxy', 0),))
        self._stub = ff_grpc.ApiStub(channel)

    def get_user(self, name: str) -> GetUser:
        user = self._stub.GetUser(metadata_pb2.Name(name=name))
        return GetUser(self._stub, user)

    def get_provider(self, name: str) -> GetProvider:
        provider = self._stub.GetProvider(
            metadata_pb2.Name(name=name))
        return GetProvider(self._stub, provider)

    def get_source(self, name: str, variant: str) -> GetSource:
        source = self._stub.GetSource(
            metadata_pb2.NameVariant(name=name, variant=variant))
        return GetSource(self._stub, source)

    def get_entity(self, name: str) -> GetEntity:
        entity = self._stub.GetEntity(
            metadata_pb2.Name(name=name))
        return GetEntity(self._stub, entity)

    def get_feature(self, name: str, variant: str) -> GetFeature:
        feature = self._stub.GetFeature(
            metadata_pb2.NameVariant(name=name, variant=variant))
        return GetFeature(self._stub, feature)

    def get_label(self, name: str, variant: str) -> GetLabel:
        label = self._stub.GetLabel(
            metadata_pb2.NameVariant(name=name, variant=variant))
        return GetLabel(self._stub, label)