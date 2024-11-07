#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from featureform.proto import metadata_pb2
from .format import *

cutoff_length = 60


def list_name(stub, resource_type):
    stub_list_functions = {
        "model": stub.ListModels,
    }
    format_rows("NAME")
    res = sorted(
        [
            received
            for received in stub_list_functions[resource_type](
                metadata_pb2.ListRequest()
            )
        ],
        key=lambda x: x.name,
    )
    for f in res:
        format_rows(f.name)
    return res


def list_name_status(stub, resource_type):
    stub_list_functions = {"entity": stub.ListEntities, "user": stub.ListUsers}
    format_rows("NAME", "STATUS")
    res = sorted(
        [
            received
            for received in stub_list_functions[resource_type](
                metadata_pb2.ListRequest()
            )
        ],
        key=lambda x: x.name,
    )
    for f in res:
        format_rows(f.name, f.status.Status._enum_type.values[f.status.status].name)
    return res


def list_name_status_desc(stub, resource_type):
    stub_list_functions = {"model": stub.ListModels, "provider": stub.ListProviders}
    format_rows("NAME", "STATUS", "DESCRIPTION")
    res = sorted(
        [
            received
            for received in stub_list_functions[resource_type](
                metadata_pb2.ListRequest()
            )
        ],
        key=lambda x: x.name,
    )
    for f in res:
        format_rows(
            f.name,
            f.status.Status._enum_type.values[f.status.status].name,
            f.description[:cutoff_length],
        )
    return res


def list_name_variant_status(stub, resource_type):
    stub_list_functions = {
        "feature": [stub.ListFeatures, stub.GetFeatureVariants],
        "label": [stub.ListLabels, stub.GetLabelVariants],
    }

    format_rows("NAME", "VARIANT", "STATUS")
    res = sorted(
        [
            received
            for received in stub_list_functions[resource_type][0](
                metadata_pb2.ListRequest()
            )
        ],
        key=lambda x: x.name,
    )
    for f in res:
        for v in f.variants:

            searchNameVariant = metadata_pb2.NameVariant(name=f.name, variant=v)
            req = metadata_pb2.NameVariantRequest(name_variant=searchNameVariant)
            for x in stub_list_functions[resource_type][1](iter([req])):
                if x.variant == f.default_variant:
                    format_rows(
                        f.name,
                        f"{f.default_variant} (default)",
                        x.status.Status._enum_type.values[x.status.status].name,
                    )
                else:
                    format_rows(
                        x.name,
                        x.variant,
                        x.status.Status._enum_type.values[x.status.status].name,
                    )
    return res


def list_name_variant_status_desc(stub, resource_type):
    stub_list_functions = {
        "source": [stub.ListSources, stub.GetSourceVariants],
        "training-set": [stub.ListTrainingSets, stub.GetTrainingSetVariants],
        "trainingset": [stub.ListTrainingSets, stub.GetTrainingSetVariants],
    }

    format_rows("NAME", "VARIANT", "STATUS", "DESCRIPTION")
    res = sorted(
        [
            received
            for received in stub_list_functions[resource_type][0](
                metadata_pb2.ListRequest()
            )
        ],
        key=lambda x: x.name,
    )
    for f in res:
        for v in f.variants:
            searchNameVariant = metadata_pb2.NameVariant(name=f.name, variant=v)
            req = metadata_pb2.NameVariantRequest(name_variant=searchNameVariant)
            for x in stub_list_functions[resource_type][1](iter([req])):
                if x.variant == f.default_variant:
                    format_rows(
                        f.name,
                        f"{f.default_variant} (default)",
                        x.status.Status._enum_type.values[x.status.status].name,
                        x.description,
                    )
                else:
                    format_rows(
                        x.name,
                        x.variant,
                        x.status.Status._enum_type.values[x.status.status].name,
                        x.description,
                    )
    return res
