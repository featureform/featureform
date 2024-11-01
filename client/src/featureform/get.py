#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from featureform.proto import metadata_pb2
import grpc
from .format import *


def get_user_info(stub, name):
    searchName = metadata_pb2.NameRequest(name=metadata_pb2.Name(name=name))
    try:
        for user in stub.GetUsers(iter([searchName])):
            format_rows("USER NAME: ", user.name)
            format_tags_and_properties(user.tags, user.properties)
            format_pg()
            format_rows("NAME", "VARIANT", "TYPE")
            for f in user.features:
                format_rows(f.name, f.variant, "feature")
            for l in user.labels:
                format_rows(l.name, l.variant, "label")
            for t in user.trainingsets:
                format_rows(t.name, t.variant, "training set")
            for s in user.sources:
                format_rows(s.name, s.variant, "source")
            format_pg()
            return user
    except grpc._channel._MultiThreadedRendezvous:
        print("User not found.")


def get_entity_info(stub, name):
    searchName = metadata_pb2.NameRequest(name=metadata_pb2.Name(name=name))
    try:
        for x in stub.GetEntities(iter([searchName])):
            format_rows(
                [
                    ("ENTITY NAME: ", x.name),
                    (
                        "STATUS: ",
                        x.status.Status._enum_type.values[x.status.status].name,
                    ),
                ]
            )
            format_tags_and_properties(x.tags, x.properties)
            format_pg()
            format_rows("NAME", "VARIANT", "TYPE")
            for f in x.features:
                format_rows(f.name, f.variant, "feature")
            for l in x.labels:
                format_rows(l.name, l.variant, "label")
            for t in x.trainingsets:
                format_rows(t.name, t.variant, "training set")
            format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Entity not found.")


def get_resource_info(stub, resource_type, name):
    stub_get_functions = {
        "feature": stub.GetFeatures,
        "label": stub.GetLabels,
        "source": stub.GetSources,
        "trainingset": stub.GetTrainingSets,
        "training-set": stub.GetTrainingSets,
        "model": stub.GetModels,
    }

    searchName = metadata_pb2.NameRequest(name=metadata_pb2.Name(name=name))
    try:
        for x in stub_get_functions[resource_type](iter([searchName])):
            rows = [("NAME: ", x.name)]
            if hasattr(x, '"status'):
                rows.append(
                    (
                        "STATUS: ",
                        x.status.Status._enum_type.values[x.status.status].name,
                    )
                )
            format_rows(rows)
            if hasattr(x, "default_variant"):
                format_pg("VARIANTS:")
                format_rows(x.default_variant, "default")
                for v in x.variants:
                    if v != x.default_variant:
                        format_rows(v, "")
                format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print(f"{resource_type} not found.")


def get_feature_variant_info(stub, name, variant):
    name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
    searchNameVariant = metadata_pb2.NameVariantRequest(name_variant=name_variant)
    try:
        for x in stub.GetFeatureVariants(iter([searchNameVariant])):
            status = x.status.Status._enum_type.values[x.status.status].name
            rows = [
                ("NAME: ", x.name),
                ("VARIANT: ", x.variant),
                ("TYPE:", x.type),
                ("ENTITY:", x.entity),
                ("OWNER:", x.owner),
                ("DESCRIPTION:", x.description),
                ("PROVIDER:", x.provider),
                ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name),
            ]
            if status == "FAILED":
                rows.append(("ERROR: ", x.status.error_message))
            format_rows(rows)
            format_tags_and_properties(x.tags, x.properties)
            format_pg("SOURCE: ")
            format_rows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
            format_pg("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Feature variant not found.")


def get_label_variant_info(stub, name, variant):
    name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
    searchNameVariant = metadata_pb2.NameVariantRequest(name_variant=name_variant)
    try:
        for x in stub.GetLabelVariants(iter([searchNameVariant])):
            status = x.status.Status._enum_type.values[x.status.status].name
            rows = [
                ("NAME: ", x.name),
                ("VARIANT: ", x.variant),
                ("TYPE:", x.type),
                ("ENTITY:", x.entity),
                ("OWNER:", x.owner),
                ("DESCRIPTION:", x.description),
                ("PROVIDER:", x.provider),
                ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name),
            ]
            if status == "FAILED":
                rows.append(("ERROR: ", x.status.error_message))
            format_rows(rows)
            format_tags_and_properties(x.tags, x.properties)
            format_pg("SOURCE: ")
            format_rows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
            format_pg("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Label variant not found.")


def get_source_variant_info(stub, name, variant):
    name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
    searchNameVariant = metadata_pb2.NameVariantRequest(name_variant=name_variant)
    try:
        for x in stub.GetSourceVariants(iter([searchNameVariant])):
            status = x.status.Status._enum_type.values[x.status.status].name
            rows = [
                ("NAME: ", x.name),
                ("VARIANT: ", x.variant),
                ("OWNER:", x.owner),
                ("DESCRIPTION:", x.description),
                ("PROVIDER:", x.provider),
                ("TABLE:", x.table),
                ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name),
            ]
            if status == "FAILED":
                rows.append(("ERROR: ", x.status.error_message))
            format_rows(rows)
            format_tags_and_properties(x.tags, x.properties)
            format_pg("DEFINITION:")
            print("TRANSFORMATION")
            print(x.transformation.SQLTransformation.query)
            format_pg("SOURCES")
            format_rows("NAME", "VARIANT")
            for s in x.transformation.SQLTransformation.source:
                format_rows(s.name, s.variant)
            format_pg("PRIMARY DATA")
            print(x.primaryData.table.name)
            print("FEATURES:")
            format_rows("NAME", "VARIANT")
            for t in x.features:
                format_rows(t.name, t.variant)
            format_pg("LABELS:")
            format_rows("NAME", "VARIANT")
            for t in x.labels:
                format_rows(t.name, t.variant)
            format_pg("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Source variant not found.")


def get_training_set_variant_info(stub, name, variant):
    name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
    searchNameVariant = metadata_pb2.NameVariantRequest(name_variant=name_variant)
    try:
        for x in stub.GetTrainingSetVariants(iter([searchNameVariant])):
            status = x.status.Status._enum_type.values[x.status.status].name
            rows = [
                ("NAME: ", x.name),
                ("VARIANT: ", x.variant),
                ("OWNER:", x.owner),
                ("DESCRIPTION:", x.description),
                ("PROVIDER:", x.provider),
                ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name),
            ]
            if status == "FAILED":
                rows.append(("ERROR: ", x.status.error_message))
            format_rows(rows)
            format_tags_and_properties(x.tags, x.properties)
            format_pg("LABEL: ")
            format_rows([("NAME", "VARIANT"), (x.label.name, x.label.variant)])
            format_pg("FEATURES:")
            format_rows("NAME", "VARIANT")
            for f in x.features:
                format_rows(f.name, f.variant)
            format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Training set variant not found.")


def get_provider_info(stub, name):
    searchName = metadata_pb2.NameRequest(name=metadata_pb2.Name(name=name))
    try:
        for x in stub.GetProviders(iter([searchName])):
            status = x.status.Status._enum_type.values[x.status.status].name
            rows = [
                ("NAME: ", x.name),
                ("DESCRIPTION: ", x.description),
                ("TYPE: ", x.type),
                ("SOFTWARE: ", x.software),
                ("TEAM: ", x.team),
                ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name),
            ]
            if status == "FAILED":
                rows.append(("ERROR: ", x.status.error_message))
            format_rows(rows)
            format_tags_and_properties(x.tags, x.properties)
            format_pg("SOURCES:")
            format_rows("NAME", "VARIANT")
            for s in x.sources:
                format_rows(s.name, s.variant)
            format_pg("FEATURES:")
            format_rows("NAME", "VARIANT")
            for f in x.features:
                format_rows(f.name, f.variant)
            format_pg("LABELS:")
            format_rows("NAME", "VARIANT")
            for l in x.labels:
                format_rows(l.name, l.variant)
            format_pg("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Provider not found.")


def format_tags_and_properties(tags, properties):
    if len(tags.tag):
        format_rows("TAGS:", ", ".join(tags.tag))
    if len(properties.property.items()):
        format_rows(
            "PROPERTIES:",
            ", ".join(
                [f"{k}:{v.string_value}" for (k, v) in properties.property.items()]
            ),
        )
