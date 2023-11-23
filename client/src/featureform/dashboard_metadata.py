import json
import os

import pandas as pd
from markupsafe import escape
from featureform import ResourceClient
from featureform.serving import LocalClientImpl
from flask import Blueprint, Response, request
from flask_cors import CORS, cross_origin
from typing import List

from .metadata_repository import (
    MetadataRepositoryLocalImpl,
    Feature,
    FeatureVariant,
    TrainingSet,
    TrainingSetVariant,
    Source,
    SourceVariant,
    Label,
    LabelVariant,
    Entity,
    Model,
    User,
    Provider,
)
from .sqlite_metadata import SQLiteMetadata
from .type_objects import (
    FeatureResource,
    FeatureVariantResource,
    TrainingSetResource,
    TrainingSetVariantResource,
    SourceResource,
    SourceVariantResource,
    EntityResource,
    UserResource,
    ModelResource,
    LabelResource,
    LabelVariantResource,
    ProviderResource,
)
from .resources import SourceType
from .version import get_package_version

path = os.path.join(os.path.dirname(__file__), "dashboard")

dashboard_app = Blueprint(
    "dashboard_app", __name__, static_folder=path + "/out/", static_url_path=""
)

CORS(dashboard_app)


@dashboard_app.route("/")
def index():
    return dashboard_app.send_static_file("index.html")


@dashboard_app.route("/<type>")
def get_type(type):
    return dashboard_app.send_static_file("[type].html")


@dashboard_app.route("/<type>/<entity>")
def entity(type, entity):
    return dashboard_app.send_static_file("[type]/[entity].html")


@dashboard_app.route("/search")
def search():
    return dashboard_app.send_static_file("query.html")


@dashboard_app.route("/query")
def query():
    return dashboard_app.send_static_file("query.html")


@dashboard_app.route("/static/<asset>")
def deliver_static(asset):
    return dashboard_app.send_static_file("static/" + asset)


@dashboard_app.route("/data/version")
def version():
    return {"version": get_package_version()}


@dashboard_app.route("/data/sourcedata", methods=["GET"])
@cross_origin(allow_headers=["Content-Type"])
def source_data():
    limit = 150
    n = 0
    name = request.args["name"]
    variant = request.args["variant"]
    if name == "" or variant == "":
        error = f"Error 400: GetSourceData - Could not find the name({name}) or variant({variant}) query parameters."
        return Response(
            response=json.dumps(error), status=400, mimetype="application/json"
        )
    try:
        with LocalClientImpl() as localClientImpl:
            source_data = {"columns": [], "rows": []}
            df = localClientImpl.get_input_df(name, variant)
            if isinstance(df, pd.Series):
                df = df.to_frame()
                df.reset_index(inplace=True)
            for column in df.columns:
                source_data["columns"].append(column)
            for _, currentRow in df.iterrows():
                n = n + 1
                if n > limit:
                    break
                currentRow = currentRow.fillna("NaN")
                source_data["rows"].append(currentRow.to_list())
            return json.dumps(source_data, allow_nan=False)
    except Exception as e:
        print(e)
        error = "Error 500: Unable to retrieve source_data columns."
        return Response(
            response=json.dumps(error), status=500, mimetype="application/json"
        )


@dashboard_app.route("/data/<type>/<resource>/gettags", methods=["POST"])
@cross_origin(allow_headers=["Content-Type"])
def get_tags(type, resource):
    try:
        response = {"name": resource, "variant": request.json["variant"], "tags": []}
        db = MetadataRepositoryLocalImpl(SQLiteMetadata())
        tags = db.get_tags_for_resource(
            escape(response["name"]), escape(response["variant"]), type
        )
        if len(tags):
            response["tags"] = json.loads(tags)
        return json.dumps(response, allow_nan=False)
    except Exception as e:
        print(e)
        error = f"Error 500: Unable to retrieve tags from resource."
        return Response(
            response=json.dumps(error), status=500, mimetype="application/json"
        )


@dashboard_app.route("/data/<type>/<resource>/tags", methods=["POST"])
@cross_origin(allow_headers=["Content-Type"])
def post_tags(type, resource):
    try:
        response = {
            "name": resource,
            "variant": request.json["variant"],
            "tags": request.json["tags"],
        }
        db = MetadataRepositoryLocalImpl(SQLiteMetadata())

        if type == "features":
            found_resource = db.get_feature_variant(
                response["name"], response["variant"]
            )
        elif type == "training_sets":
            found_resource = db.get_training_set_variant(
                response["name"], response["variant"]
            )
        elif type == "sources":
            found_resource = db.get_source_variant(
                response["name"], response["variant"]
            )
        elif type == "labels":
            found_resource = db.get_label_variant(response["name"], response["variant"])
        elif type == "entities":
            found_resource = db.get_entity(response["name"])
        elif type == "models":
            found_resource = db.get_model(response["name"])
        elif type == "users":
            found_resource = db.get_user(response["name"])
        elif type == "providers":
            found_resource = db.get_provider(response["name"])

            found_resource.tags = escape(response["tags"])
            db.update_resource(found_resource)
        return json.dumps(response, allow_nan=False)
    except Exception as e:
        print(e)
        error = f"Error 500: Unable to post tags to resource."
        return Response(
            response=json.dumps(error), status=500, mimetype="application/json"
        )


@dashboard_app.route("/data/<type>", methods=["POST", "GET"])
@cross_origin(allow_headers=["Content-Type"])
def get_metadata_list(type):
    type = type.replace("-", "_")
    if type not in [
        "features",
        "training_sets",
        "sources",
        "labels",
        "entities",
        "models",
        "users",
        "providers",
    ]:
        error_data = f"Invalid resource type: {type}"
        response = Response(
            response=json.dumps(error_data), status=400, mimetype="application/json"
        )
        return response

    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    metadata_object_list = []
    if type == "features":
        for feature in db.get_features():
            metadata_object_list.append(build_feature_resource(feature))
    elif type == "training_sets":
        for training_set in db.get_training_sets():
            metadata_object_list.append(build_training_set_resource(training_set))
    elif type == "sources":
        for source in db.get_sources():
            metadata_object_list.append(build_source_resource(source))
    elif type == "labels":
        for label in db.get_labels():
            metadata_object_list.append(build_label_resource(label))
    elif type == "entities":
        for entity in db.get_entities():
            metadata_object_list.append(build_entity_resource(entity))
    elif type == "models":
        for model in db.get_models():
            metadata_object_list.append(build_model_resource(model))
    elif type == "users":
        for user in db.get_users():
            metadata_object_list.append(build_user_resource(user))
    elif type == "providers":
        for provider in db.get_providers():
            metadata_object_list.append(build_provider_resource(provider))

    response = Response(
        response=json.dumps(metadata_object_list),
        status=200,
        mimetype="application/json",
    )
    return response


@dashboard_app.route("/data/<type>/<resource>", methods=["POST", "GET"])
@cross_origin(allow_headers=["Content-Type"])
def get_metadata(type, resource):
    type = type.replace("-", "_")
    if type not in [
        "features",
        "training_sets",
        "sources",
        "labels",
        "entities",
        "models",
        "users",
        "providers",
    ]:
        error_data = f"Invalid resource type: {type}"
        response = Response(
            response=json.dumps(error_data), status=400, mimetype="application/json"
        )
        return response

    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    metadata_object = {}
    if type == "features":
        records = db.get_features()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_feature_resource(records[0])
    elif type == "training_sets":
        records = db.get_training_sets()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_training_set_resource(records[0])
    elif type == "sources":
        records = db.get_sources()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_source_resource(records[0])
    elif type == "labels":
        records = db.get_labels()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_label_resource(records[0])
    elif type == "entities":
        records = db.get_entities()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_entity_resource(records[0])
    elif type == "models":
        records = db.get_models()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_model_resource(records[0])
    elif type == "users":
        records = db.get_users()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_user_resource(records[0])
    elif type == "providers":
        records = db.get_providers()
        records = list(filter(lambda rec: rec.name == resource, records))
        if len(records) > 0:
            metadata_object = build_provider_resource(records[0])
    return Response(
        response=json.dumps(metadata_object), status=200, mimetype="application/json"
    )


@dashboard_app.route("/data/search", methods=["GET"])
@cross_origin(allow_headers=["Content-Type"])
def search_metadata():
    raw_query = request.args["q"]
    rc = ResourceClient(local=True)
    results = rc.search(raw_query, True)
    payload = []
    for r in results:
        payload.append(
            {"Name": r["name"], "Variant": r["variant"], "Type": r["resource_type"]}
        )

    return json.dumps(payload)


def variant_list_to_dict(variant_list):
    """Convert a variant list, into a dictionary with variant names as the dictionary keys"""
    variants_dict = dict()

    for variant in variant_list:
        variant_name = variant["variant"]
        variants_dict[variant_name] = variant

    return variants_dict


def resources_list_to_dict(resource_list):
    """Convert a resource list, into a dictionary with resource names as the dictionary keys"""
    variants_dict = dict()

    for variant in resource_list:
        resource_name = variant["name"]
        if resource_name in variants_dict:
            variants_dict[resource_name].append(variant)
        else:
            variants_dict[resource_name] = [variant]

    return variants_dict


def name_variant_list_to_dict_list(input_list_param: List[dict]):
    """Convert a name variant list, into a dictionary list"""
    input_list = list()

    for name_variant in input_list_param:
        input_list.append(
            {"Name": name_variant["name"], "Variant": name_variant["variant"]}
        )
    return input_list


def build_feature_resource(feature_main: Feature):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    variant_list = []
    for variant_name in feature_main.variants:
        found_variant = db.get_feature_variant(
            name=feature_main.name, variant=variant_name
        )
        variant_list.append(build_feature_variant_resource(found_variant))

    return FeatureResource(
        name=feature_main.name,
        defaultVariant=feature_main.default_variant,
        type="Feature",
        allVariants=feature_main.variants,
        variants=variant_list_to_dict(variant_list),
    ).to_dictionary()


def build_feature_variant_resource(variant_data: FeatureVariant):
    definition = ""
    if variant_data.additional_parameters is not None:
        definition = variant_data.additional_parameters.definition

    feature_variant_resource = FeatureVariantResource(
        created=variant_data.created,
        description=variant_data.description,
        entity=variant_data.entity,
        name=variant_data.name,
        owner=variant_data.owner,
        provider=variant_data.provider,
        dataType=variant_data.value_type,
        variant=variant_data.variant,
        status=variant_data.status,
        location={
            "entity": variant_data.location.entity,
            "value": variant_data.location.value,
            "timestamp": variant_data.location.timestamp,
        },
        source={
            "Name": variant_data.source[0],
            "Variant": variant_data.source[1],
        },
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else {},
        definition=definition,
    ).to_dictionary()

    return feature_variant_resource


def build_training_set_resource(training_set_main: TrainingSet):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    variant_list = []
    for variant_name in training_set_main.variants:
        found_variant = db.get_training_set_variant(
            name=training_set_main.name, variant=variant_name
        )
        variant_list.append(build_training_set_variant_resource(found_variant))

    return TrainingSetResource(
        name=training_set_main.name,
        defaultVariant=training_set_main.default_variant,
        type="TrainingSet",
        allVariants=training_set_main.variants,
        variants=variant_list_to_dict(variant_list),
    ).to_dictionary()


def build_training_set_variant_resource(variant_data: TrainingSetVariant):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    training_set_feature_list = []
    tsf_association_list = db.get_training_set_features(
        name=variant_data.name, variant=variant_data.variant
    )
    for tsf in tsf_association_list:
        found_feature_variant = db.get_feature_variant(
            name=tsf.feature_name, variant=tsf.feature_variant
        )
        training_set_feature_list.append(
            build_feature_variant_resource(found_feature_variant)
        )

    training_set_variant_resource = TrainingSetVariantResource(
        created=variant_data.created if variant_data.created is not None else "",
        description=variant_data.description,
        name=variant_data.name,
        owner=variant_data.owner,
        variant=variant_data.variant,
        label={
            "Name": variant_data.label[0],
            "Variant": variant_data.label[1],
        },
        status=variant_data.status,
        features=resources_list_to_dict(training_set_feature_list),
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else [],
    ).to_dictionary()

    return training_set_variant_resource


def build_source_resource(source_main: Source):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    variant_list = []
    for variant_name in source_main.variants:
        found_variant = db.get_source_variant(
            name=source_main.name, variant=variant_name
        )
        variant_list.append(build_source_variant_resource(found_variant))

    return SourceResource(
        name=source_main.name,
        defaultVariant=source_main.default_variant,
        type="Source",
        allVariants=source_main.variants,
        variants=variant_list_to_dict(variant_list),
    ).to_dictionary()


def build_source_variant_resource(variant_data: SourceVariant):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())

    training_set_variant_resource_list = []
    feature_variant_resource_list = []
    try:
        feature_list = db.get_feature_variants_from_source(
            source_name=variant_data.name, source_variant=variant_data.variant
        )
    except ValueError:
        feature_list = []
    for curr_feature in feature_list:
        try:
            # build the feature variant resource
            found_feature_variant = db.get_feature_variant(
                curr_feature.name, curr_feature.variant
            )
            feature_variant_resource_list.append(
                build_feature_variant_resource(found_feature_variant)
            )

            # find the feature training sets, add to trainint set()
            training_set_feature_table_list = db.get_training_set_from_features(
                found_feature_variant.name, found_feature_variant.variant
            )

            for training_set in training_set_feature_table_list:
                found_training_set = db.get_training_set_variant(
                    training_set.training_set_name, training_set.training_set_variant
                )
                training_set_variant_resource_list.append(
                    build_training_set_variant_resource(found_training_set)
                )
        except ValueError:
            continue

    label_variant_resource_list = []
    try:
        label_list = db.get_label_variants_from_source(
            source_name=variant_data.name, source_variant=variant_data.variant
        )
    except:
        label_list = []
    for curr_label in label_list:
        try:
            # build the label variant resource
            label_variant_resource_list.append(build_label_variant_resource(curr_label))

            training_set_variant_list = db.get_training_set_variant_from_label(
                label_name=curr_label.name, label_variant=curr_label.variant
            )

            # only add if not in list. set not hashable
            for tsv in training_set_variant_list:
                if (
                    next(
                        (
                            item
                            for item in training_set_variant_resource_list
                            if (
                                item["name"] == tsv.name
                                and item["variant"] == tsv.variant
                            )
                        ),
                        None,
                    )
                    == None
                ):
                    training_set_variant_resource_list.append(
                        build_training_set_variant_resource(tsv)
                    )
        except ValueError:
            continue

    definition_text = ""
    if variant_data.transformation == SourceType.DF_TRANSFORMATION.value:
        definition_text = db.get_source_variant_text(
            name=variant_data.name, variant=variant_data.variant
        )
    else:
        definition_text = variant_data.definition

    source_variant_resource = SourceVariantResource(
        created=variant_data.created,
        description=variant_data.description,
        name=variant_data.name,
        owner=variant_data.owner,
        definition=definition_text,
        provider=variant_data.provider,
        sourceType=variant_data.source_type,
        variant=variant_data.variant,
        labels=resources_list_to_dict(label_variant_resource_list),
        status=variant_data.status,
        features=resources_list_to_dict(feature_variant_resource_list),
        trainingSets=resources_list_to_dict(training_set_variant_resource_list),
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else [],
        inputs=name_variant_list_to_dict_list(variant_data.inputs),
    ).to_dictionary()

    return source_variant_resource


def build_label_resource(label_main: Label):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    variant_list = []
    for variant_name in label_main.variants:
        found_variant = db.get_label_variant(name=label_main.name, variant=variant_name)
        variant_list.append(build_label_variant_resource(found_variant))

    return LabelResource(
        name=label_main.name,
        defaultVariant=label_main.default_variant,
        type="Label",
        allVariants=label_main.variants,
        variants=variant_list_to_dict(variant_list),
    ).to_dictionary()


def build_label_variant_resource(variant_data: LabelVariant):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())

    label_training_set_list = []
    training_set_variant_list = db.get_training_set_variant_from_label(
        label_name=variant_data.name, label_variant=variant_data.variant
    )
    for tsv in training_set_variant_list:
        label_training_set_list.append(build_training_set_variant_resource(tsv))

    label_variant_resource = LabelVariantResource(
        created=variant_data.created,
        description=variant_data.description,
        name=variant_data.name,
        owner=variant_data.owner,
        variant=variant_data.variant,
        entity=variant_data.entity,
        dataType=variant_data.value_type,
        status=variant_data.status,
        source={
            "Name": variant_data.source[0],
            "Variant": variant_data.source[1],
        },
        location={
            "entity": variant_data.location.entity,
            "value": variant_data.location.value,
            "timestamp": variant_data.location.timestamp,
        },
        trainingSets=resources_list_to_dict(label_training_set_list),
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else [],
    ).to_dictionary()
    return label_variant_resource


def build_entity_resource(entity_main: Entity):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())

    entity_feature_list = []
    feature_list = db.get_features()
    for current_feature in feature_list:
        for variant_name in current_feature.variants:
            found_feature_variant = db.get_feature_variant(
                name=current_feature.name, variant=variant_name
            )
            if found_feature_variant.entity == entity_main.name:
                entity_feature_list.append(
                    build_feature_variant_resource(found_feature_variant)
                )

    entity_labels_list = []
    label_list = db.get_labels()
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_label_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            if found_label_variant.entity == entity_main.name:
                entity_labels_list.append(
                    build_label_variant_resource(found_label_variant)
                )

    entity_training_set_list = []
    training_set_list = db.get_training_sets()
    for current_training_set in training_set_list:
        for variant_name in current_training_set.variants:
            found_training_set_variant = db.get_training_set_variant(
                name=current_training_set.name, variant=variant_name
            )
            entity_training_set_list.append(
                build_training_set_variant_resource(found_training_set_variant)
            )

    return EntityResource(
        name=entity_main.name,
        type=entity_main.type().capitalize(),
        description=entity_main.description,
        status=entity_main.status,
        features=resources_list_to_dict(entity_feature_list),
        labels=resources_list_to_dict(entity_labels_list),
        trainingSets=resources_list_to_dict(entity_training_set_list),
        tags=entity_main.tags if entity_main.tags is not None else [],
        properties=entity_main.properties if entity_main.properties is not None else [],
    ).to_dictionary()


def build_model_resource(model_obj: Model):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())

    model_feature_list = []
    feature_list = db.get_features()
    for current_feature in feature_list:
        for variant_name in current_feature.variants:
            found_variant = db.get_feature_variant(
                name=current_feature.name, variant=variant_name
            )
            if found_variant.name == model_obj.name:
                model_feature_list.append(build_feature_variant_resource(found_variant))

    model_label_list = []
    label_list = db.get_labels()
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            if found_variant.variant == model_obj.name:
                model_label_list.append(build_label_variant_resource(found_variant))

    model_training_set_list = []
    training_set_list = db.get_training_sets()
    for current_training_set in training_set_list:
        for variant_name in current_training_set.variants:
            found_variant = db.get_training_set_variant(
                name=current_training_set.name, variant=variant_name
            )
            if found_variant.name == model_obj.name:
                model_training_set_list.append(
                    build_training_set_variant_resource(found_variant)
                )

    return ModelResource(
        name=model_obj.name,
        type="Model",
        description=model_obj.description,
        features=resources_list_to_dict(model_feature_list),
        labels=resources_list_to_dict(model_label_list),
        trainingSets=resources_list_to_dict(model_training_set_list),
        tags=model_obj.tags if model_obj.tags is not None else [],
        properties=model_obj.properties if model_obj.properties is not None else [],
    ).to_dictionary()


def build_user_resource(user_obj: User):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())

    user_feature_list = []
    feature_list = db.get_features()
    for current_feature in feature_list:
        for variant_name in current_feature.variants:
            found_variant = db.get_feature_variant(
                name=current_feature.name, variant=variant_name
            )
            if found_variant.owner == user_obj.name:
                user_feature_list.append(build_feature_variant_resource(found_variant))

    user_label_list = []
    label_list = db.get_labels()
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            if found_variant.owner == user_obj.name:
                user_label_list.append(build_label_variant_resource(found_variant))

    user_training_set_list = []
    training_set_list = db.get_training_sets()
    for current_training_set in training_set_list:
        for variant_name in current_training_set.variants:
            found_variant = db.get_training_set_variant(
                name=current_training_set.name, variant=variant_name
            )
            if found_variant.owner == user_obj.name:
                user_training_set_list.append(
                    build_training_set_variant_resource(found_variant)
                )

    user_source_list = []
    source_list = db.get_sources()
    for current_source in source_list:
        for variant_name in current_source.variants:
            found_variant = db.get_source_variant(
                name=current_source.name, variant=variant_name
            )
            if found_variant.owner == user_obj.name:
                user_source_list.append(build_source_variant_resource(found_variant))

    return UserResource(
        name=user_obj.name,
        type="User",
        status=user_obj.status,
        features=resources_list_to_dict(user_feature_list),
        labels=resources_list_to_dict(user_label_list),
        trainingSets=resources_list_to_dict(user_training_set_list),
        sources=resources_list_to_dict(user_source_list),
        tags=user_obj.tags if user_obj.tags is not None else [],
        properties=user_obj.properties if user_obj.properties is not None else [],
    ).to_dictionary()


def build_provider_resource(provider_obj: Provider):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    provider_source_list = []
    source_list = db.get_sources()
    for current_source in source_list:
        for variant_name in current_source.variants:
            found_variant = db.get_source_variant(
                name=current_source.name, variant=variant_name
            )
            if found_variant.provider == provider_obj.name:
                provider_source_list.append(
                    build_source_variant_resource(found_variant)
                )

    provider_feature_list = []
    feature_list = db.get_features()
    for current_feature in feature_list:
        for variant_name in current_feature.variants:
            found_variant = db.get_feature_variant(
                name=current_feature.name, variant=variant_name
            )
            if found_variant.provider == provider_obj.name:
                provider_feature_list.append(
                    build_feature_variant_resource(found_variant)
                )

    provider_label_list = []
    label_list = db.get_labels()
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            if found_variant.provider == provider_obj.name:
                provider_label_list.append(build_label_variant_resource(found_variant))

    return ProviderResource(
        name=provider_obj.name,
        type="Provider",
        description=provider_obj.description,
        providerType=provider_obj.config.type(),
        software=provider_obj.config.software(),
        team=provider_obj.team,
        sources=resources_list_to_dict(provider_source_list),
        status=provider_obj.status,
        features=resources_list_to_dict(provider_feature_list),
        labels=resources_list_to_dict(provider_label_list),
        tags=provider_obj.tags if provider_obj.tags is not None else [],
        properties=provider_obj.properties
        if provider_obj.properties is not None
        else [],
    ).to_dictionary()
