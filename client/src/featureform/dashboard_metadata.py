import json
import os

import datetime
import pandas as pd
from featureform import ResourceClient
from featureform.serving import LocalClientImpl
from flask import Blueprint, Response, request
from flask_cors import CORS, cross_origin
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
from .resources import SourceType
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
def type(type):
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
        error = f"Error 500: Unable to retrieve source_data columns."
        return Response(
            response=json.dumps(error), status=500, mimetype="application/json"
        )


@dashboard_app.route("/data/<type>/<resource>/gettags", methods=["POST"])
@cross_origin(allow_headers=["Content-Type"])
def get_tags(type, resource):
    try:
        response = {"name": resource, "variant": request.json["variant"], "tags": []}
        db = MetadataRepositoryLocalImpl(SQLiteMetadata())
        tags = db.get_tags_for_resource(response["name"], response["variant"], type)
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

            found_resource.tags = response["tags"]
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
    all_data = []
    if type == "features":
        all_data.append(db.get_features())
    elif type == "training_sets":
        all_data.append(db.get_training_sets())
    elif type == "sources":
        all_data.append(db.get_sources())
    elif type == "labels":
        all_data.append(db.get_labels())
    elif type == "entities":
        all_data.append(db.get_entities())
    elif type == "models":
        all_data.append(db.get_models())
    elif type == "users":
        all_data.append(db.get_users())
    elif type == "providers":
        all_data.append(db.get_providers())

    response = Response(
        response=json.dumps(all_data), status=200, mimetype="application/json"
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
    data_as_list = []
    if type == "features":
        for feature in db.get_features():
            data_as_list.append(collect_features(feature))
    elif type == "training_sets":
        for training_set in db.get_training_sets():
            data_as_list.append(collect_training_sets(training_set))
    elif type == "sources":
        for source in db.get_sources():
            data_as_list.append(collect_sources(source))
    elif type == "labels":
        for label in db.get_labels():
            data_as_list.append(collect_labels(label))
    elif type == "entities":
        for entity in db.get_entities():
            data_as_list.append(collect_entities(entity))
    elif type == "models":
        for model in db.get_models():
            data_as_list.append(collect_models(model))
    elif type == "users":
        for user in db.get_users():
            data_as_list.append(collect_users(user))
    elif type == "providers":
        for provider in db.get_providers():
            data_as_list.append(collect_providers(provider))

    return Response(
        response=json.dumps(data_as_list), status=200, mimetype="application/json"
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


def collect_features(feature_main: Feature):
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
        variants=variant_list,
    ).to_dictionary()


def build_feature_variant_resource(variant_data: FeatureVariant):
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
            "Name": variant_data.source[0],  # todox: should be a prop instead of tuple?
            "Variant": variant_data.source[1],
        },
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else {},
    ).to_dictionary()

    return feature_variant_resource


def collect_training_sets(training_set_main: TrainingSet):
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
        variants=variant_list,
    ).to_dictionary()


def build_training_set_variant_resource(variant_data: TrainingSetVariant):
    training_set_variant_resource = TrainingSetVariantResource(
        created=variant_data.created,
        description=variant_data.description,
        name=variant_data.name,
        owner=variant_data.owner,
        variant=variant_data.variant,
        label={
            "Name": variant_data.label[0],  # todox: should be a prop instead of tuple?
            "Variant": variant_data.label[1],
        },
        status=variant_data["status"],
        features=[],
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else [],
    ).to_dictionary()

    return training_set_variant_resource


def collect_sources(source_main: Source):
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
        variants=variant_list,
    ).to_dictionary()


def build_source_variant_resource(variant_data: SourceVariant):
    source_variant_resource = SourceVariantResource(
        created=variant_data.created,
        description=variant_data.description,
        name=variant_data.name,
        owner=variant_data.owner,
        variant=variant_data.variant,
        labels=[],
        status=variant_data.status,
        features=[],
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else [],
    ).to_dictionary()

    return source_variant_resource


def collect_labels(label_main: Label):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    variant_list = []
    for variant_name in label_main.variants:
        found_variant = db.get_label_variant(name=label_main.name, variant=variant_name)
        variant_list.append(build_label_variant_resource(found_variant))

    return LabelResource(
        name=label_main.name,
        defaultVariant=label_main.default_variant,
        type="Feature",
        allVariants=label_main.variants,
        variants=variant_list,
    ).to_dictionary()


def build_label_variant_resource(variant_data: LabelVariant):
    label_variant_resource = LabelVariantResource(
        created=datetime.datetime.now(),  # todox: missing field
        description=variant_data.description,
        name=variant_data.name,
        owner=variant_data.owner,
        variant=variant_data.variant,
        status=variant_data["status"],
        features=[],
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else [],
    ).to_dictionary()
    return label_variant_resource


def collect_entities(entity_main: Entity):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    entity_labels_list = []
    label_list = db.get_labels()
    # todox: not a fan of the O2 loop. may need a 1-off custome sql method
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_label_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            if found_label_variant.entity == entity_main.name:
                entity_labels_list.append(found_label_variant)
    entity_training_set_list = []
    training_set_list = db.get_training_sets()
    # todox: same issue as above
    for current_training_set in training_set_list:
        for variant_name in current_training_set.variants:
            found_training_set_variant = db.get_training_set_variant(
                name=current_training_set.name, variant=variant_name
            )
            entity_training_set_list.append(found_training_set_variant)
    return EntityResource(
        name=entity_main.name,
        type=entity_main.type,
        description=entity_main.description,
        status=entity_main.status,
        features=[],
        labels=entity_labels_list,
        trainingSets=entity_training_set_list,
        tags=entity_main.tags if entity_main.tags is not None else [],
        properties=entity_main.properties if entity_main.properties is not None else [],
    ).to_dictionary()


def collect_models(model_obj: Model):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())

    model_feature_list = []
    feature_list = db.get_features()
    for current_feature in feature_list:
        for variant_name in current_feature.variants:
            found_variant = db.get_feature_variant(
                name=current_feature.name, variant=variant_name
            )
            model_feature_list.append(found_variant)

    model_label_list = []
    label_list = db.get_labels()
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            model_label_list.append(found_variant)

    model_training_set_list = []
    training_set_list = db.get_training_sets()
    for current_training_set in training_set_list:
        for variant_name in current_training_set.variants:
            found_variant = db.get_training_set_variant(
                name=current_training_set.name, variant=variant_name
            )
            model_training_set_list.append(found_variant)

    return ModelResource(
        name=model_obj.name,
        type="Model",
        description=model_obj.description,
        features=feature_list,
        labels=label_list,
        trainingSets=training_set_list,
        tags=model_obj.tags if model_obj.tags is not None else [],
        properties=model_obj.properties if model_obj.properties is not None else [],
    ).to_dictionary()


def collect_users(user_obj: User):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())

    model_feature_list = []
    feature_list = db.get_features()
    for current_feature in feature_list:
        for variant_name in current_feature.variants:
            found_variant = db.get_feature_variant(
                name=current_feature.name, variant=variant_name
            )
            model_feature_list.append(found_variant)

    model_label_list = []
    label_list = db.get_labels()
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            model_label_list.append(found_variant)

    model_training_set_list = []
    training_set_list = db.get_training_sets()
    for current_training_set in training_set_list:
        for variant_name in current_training_set.variants:
            found_variant = db.get_training_set_variant(
                name=current_training_set.name, variant=variant_name
            )
            model_training_set_list.append(found_variant)

    return UserResource(
        name=user_obj.name,
        type="Model",
        status=user_obj.status,
        features=feature_list,
        labels=label_list,
        trainingSets=training_set_list,
        tags=user_obj.tags if user_obj.tags is not None else [],
        properties=user_obj.properties if user_obj.properties is not None else [],
    ).to_dictionary()


def collect_providers(provider_obj: Provider):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    provider_source_list = []
    source_list = db.get_sources()
    for current_source in source_list:
        for variant_name in current_source.variants:
            found_variant = db.get_source_variant(
                name=current_source.name, variant=variant_name
            )
            provider_source_list.append(found_variant)

    provider_feature_list = []
    feature_list = db.get_features()
    for current_feature in feature_list:
        for variant_name in current_feature.variants:
            found_variant = db.get_feature_variant(
                name=current_feature.name, variant=variant_name
            )
            provider_feature_list.append(found_variant)

    provider_label_list = []
    label_list = db.get_labels()
    for current_label in label_list:
        for variant_name in current_label.variants:
            found_variant = db.get_label_variant(
                name=current_label.name, variant=variant_name
            )
            provider_label_list.append(found_variant)

    return ProviderResource(
        name=provider_obj.name,
        type=provider_obj.type,
        description=provider_obj.description,
        providerType=provider_obj.type,
        software=provider_obj.software,
        team=provider_obj.team,
        sources=provider_source_list,
        status=provider_obj.status,
        serializedConfig=provider_obj.config,
        features=provider_feature_list,
        labels=provider_label_list,
        tags=provider_obj.tags if provider_obj.tags is not None else [],
        properties=provider_obj.properties
        if provider_obj.properties is not None
        else [],
    ).to_dictionary()
