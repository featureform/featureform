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
        error = f"Error 500: {e}"
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
        error = f"Error 500: {e}"
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
        error = f"Error 500: {e}"
        return Response(
            response=json.dumps(error), status=500, mimetype="application/json"
        )


def variant_organiser(all_variant_list):
    variants_dict = dict()

    for variant in all_variant_list:
        name = variant["name"]
        if name in variants_dict:
            variants_dict[name].append(variant)
        else:
            variants_dict[name] = [variant]

    return variants_dict


def build_feature_variant_resource(variant_data: FeatureVariant):
    feature_variant_resource = FeatureVariantResource(
        created=datetime.datetime.now(),  # todox: missing field
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


def build_training_set_variant_resource(variant_data: TrainingSetVariant):
    training_set_variant_resource = TrainingSetVariantResource(
        created=datetime.datetime.now(),  # todox: missing field
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


def getTrainingSetFeatures(feature_list):
    feature_variant_tuple = []
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    for feature in feature_list:
        feature_variant_tuple.append(
            db.get_feature_variant(feature["feature_name"], feature["feature_variant"])
        )
    return feature_variant_tuple


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


def source_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    for variantRow in variantData:
        try:
            feature_list = db.get_source_variant(
                variantRow["name"], variantRow["variant"]
            )
        except ValueError:
            feature_list = []
        training_set_list = set()
        for f in feature_list:
            try:
                # todox: this method grabs training sets, but starting at the feature level.
                # verify that we can call 'get_training_set_variant' direclty using the feature name + variant
                features_training_set_list = db.get_training_set_from_features(
                    f["name"], f["variant"]
                )
                for training_set in features_training_set_list:
                    training_set_list.add(
                        db.get_training_set_variant(
                            training_set["training_set_name"],
                            training_set["training_set_variant"],
                        )
                    )
            except ValueError:
                continue
        try:
            label_list = db.get_labels()
            label_list = label_list.filter(
                lambda x: x["name"] == variantRow["name"]
                and x["variant"] == variantRow["variant"],
                label_list,
            )
        except ValueError:
            label_list = []
        for l in label_list:
            try:
                training_set_list = db.get_training_sets()
                training_set_list = training_set_list = training_set_list.filter(
                    lambda x: x["name"] == l["name"] and x["variant"] == l["variant"]
                )

                for training_set in training_set_list:
                    training_set_list.add(training_set)
            except ValueError:
                continue
        if variantRow["transformation"] == SourceType.DF_TRANSFORMATION.value:
            # todox: replace method
            definition = db.get_source_variant_text(
                variantRow["name"], variantRow["variant"]
            )
        else:
            definition = variantRow["definition"]
            sourceVariant = SourceVariantResource(
                variantRow["created"],
                variantRow["description"],
                variantRow["name"],
                variantRow["source_type"],
                variantRow["owner"],
                variantRow["provider"],
                variantRow["variant"],
                variantRow["status"],
                definition,
                variant_organiser(label_variant(label_list)[2]),
                variant_organiser(feature_variant(feature_list)[2]),
                variant_organiser(training_set_variant(training_set_list)[2]),
                json.loads(variantRow["tags"])
                if variantRow["tags"] is not None
                else [],
                json.loads(variantRow["properties"])
                if variantRow["properties"] is not None
                else {},
            ).to_dictionary()
            allVariantList.append(variantRow["variant"])
            variantDict[variantRow["variant"]] = sourceVariant
            variants.append(sourceVariant)

        return variantDict, allVariantList, variants


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
        created=datetime.datetime.now(),  # todox: missing field
        description=variant_data.description,
        name=variant_data.name,
        owner=variant_data.owner,
        variant=variant_data.variant,
        label={
            "Name": "todox",  # todox
            "Variant": "todox",
        },
        status=variant_data.status,
        features=[],
        tags=variant_data.tags if variant_data.tags is not None else [],
        properties=variant_data.properties
        if variant_data.properties is not None
        else [],
    ).to_dictionary()

    return source_variant_resource


def label_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []

    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    for variantRow in variantData:
        labelVariant = LabelVariantResource(
            variantRow["created"],
            variantRow["description"],
            variantRow["entity"],
            variantRow["name"],
            variantRow["owner"],
            variantRow["provider"],
            variantRow["data_type"],
            variantRow["variant"],
            {
                "entity": variantRow["source_entity"],
                "value": variantRow["source_value"],
                "timestamp": variantRow["source_timestamp"],
            },
            variantRow["status"],
            {
                "Name": variantRow["source_name"],
                "Variant": variantRow["source_variant"],
            },
            variant_organiser(
                training_set_variant(
                    # todox: verify this return
                    db.get_training_set_variant(
                        variantRow["name"], variantRow["variant"]
                    )
                )
            ),
            json.loads(variantRow["tags"]) if variantRow["tags"] is not None else [],
            json.loads(variantRow["properties"])
            if variantRow["properties"] is not None
            else {},
        ).to_dictionary()

        allVariantList.append(variantRow["variant"])
        variantDict[variantRow["variant"]] = labelVariant
        variants.append(labelVariant)
        return variantDict, allVariantList, variants


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
        label={
            "Name": "todox",  # todox: should be a prop instead of tuple?
            "Variant": "todox",
        },
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
    try:
        entity_labels_list = []
        label_list = db.get_labels()
        # todox: not a fan of the O2 loop. may need a 1-off customer sql method
        for current_label in label_list:
            for variant_name in current_label.variants:
                found_label_variant = db.get_label_variant(
                    name=current_label.name, variant=variant_name
                )
                if found_label_variant.entity == entity_main.name:
                    entity_labels_list.append(found_label_variant)
    except Exception as e:
        print(f"No labels found for entity {entity_main.name} - {e}")
        entity_labels_list = []
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
        description=entity_main["description"],
        status=entity_main["status"],
        features=[],
        labels=entity_labels_list,
        trainingSets=entity_training_set_list,
        tags=entity_main.tags if entity_main.tags is not None else [],
        properties=entity_main.properties if entity_main.properties is not None else [],
    ).to_dictionary()


def models(rowData):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    feature_variant_list = db.get_features()
    feature_variant_list = feature_variant_list.filter(
        lambda x: x["name"] == rowData["name"]
    )
    label_variant_list = db.get_labels()
    label_variant_list = label_variant_list.filter(
        lambda x: x["name"] == rowData["name"]
    )
    training_set_variant_list = db.get_labels()
    training_set_variant_list = training_set_variant_list.filter(
        lambda x: x["name"] == rowData["name"]
    )

    return ModelResource(
        rowData["name"],
        "Model",
        rowData["description"],
        rowData["status"],
        variant_organiser(feature_variant_list),
        variant_organiser(label_variant_list),
        variant_organiser(training_set_variant_list),
        json.loads(rowData["tags"]) if rowData["tags"] is not None else [],
        json.loads(rowData["properties"]) if rowData["properties"] is not None else {},
    ).to_dictionary()


def users(rowData):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    feature_variant_list = db.get_features()
    feature_variant_list = feature_variant_list.filter(
        lambda x: x["name"] == rowData["name"]
    )
    label_variant_list = db.get_labels()
    label_variant_list = label_variant_list.filter(
        lambda x: x["name"] == rowData["name"]
    )
    training_set_variant_list = db.get_labels()
    training_set_variant_list = training_set_variant_list.filter(
        lambda x: x["name"] == rowData["name"]
    )

    # todox: replace db method
    return UserResource(
        rowData["name"],
        rowData["type"],
        rowData["status"],
        variant_organiser(feature_variant_list),
        variant_organiser(label_variant_list),
        variant_organiser(training_set_variant_list),
        variant_organiser(
            source_variant(
                db.query_resource_variant(
                    "source_variant", "owner", rowData["name"]
                )  # todox: get source variants method needed again
            )[2]
        ),
        json.loads(rowData["tags"]) if rowData["tags"] is not None else [],
        json.loads(rowData["properties"]) if rowData["properties"] is not None else {},
    ).to_dictionary()


def providers(rowData):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    try:
        source_list = db.query_resource_variant(
            "source_variant", "provider", rowData["name"]
        )
    except ValueError:
        source_list = []
    try:
        feature_list = db.query_resource_variant(
            "feature_variant", "provider", rowData["name"]
        )
    except ValueError:
        feature_list = []
    try:
        label_list = db.query_resource_variant(
            "label_variant", "provider", rowData["name"]
        )
    except ValueError:
        label_list = []
    return ProviderResource(
        rowData["name"],
        rowData["type"],
        rowData["description"],
        rowData["provider_type"],
        rowData["software"],
        rowData["team"],
        variant_organiser(source_variant(source_list)[2]),
        rowData["status"],
        rowData["serialized_config"],
        variant_organiser(feature_variant(feature_list)[2]),
        variant_organiser(label_variant(label_list)[2]),
        json.loads(rowData["tags"]) if rowData["tags"] is not None else [],
        json.loads(rowData["properties"]) if rowData["properties"] is not None else {},
    ).to_dictionary()


@dashboard_app.route("/data/<type>", methods=["POST", "GET"])
@cross_origin(allow_headers=["Content-Type"])
def GetMetadataList(type):
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
        errorData = f"invalid resource type: {type}"
        response = Response(
            response=json.dumps(errorData), status=400, mimetype="application/json"
        )
        return response
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: test each return. refactor the features(), entities(), etc helper methods.
    # new impl db.get_features returns a Feature obj: name, default_variant, variants List[str].
    # each helper method expects more data than what the new repo provides from the base resource list (non-variant list)
    allData = []
    if type == "features":
        all_features = db.get_features()
        for feature in all_features:
            allData.append(collect_features(feature))

    # elif type == "training_sets":
    #     allData.append(training_sets(db.get_training_sets()))
    # elif type == "sources":
    #     allData.append(sources(db.get_sources()))
    # elif type == "labels":
    #     allData.append(labels(db.get_labels()))
    # elif type == "entities":
    #     allData.append(entities(db.get_entities()))
    # elif type == "models":
    #     allData.append(models(db.get_models()))
    # elif type == "users":
    #     allData.append(users(db.get_users()))
    # elif type == "providers":
    #     allData.append(providers(db.get_providers()))

    response = Response(
        response=json.dumps(allData), status=200, mimetype="application/json"
    )
    return response


@dashboard_app.route("/data/<type>/<resource>", methods=["POST", "GET"])
@cross_origin(allow_headers=["Content-Type"])
def GetMetadata(type, resource):
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
        errorData = f"invalid resource type: {type}"
        response = Response(
            response=json.dumps(errorData), status=400, mimetype="application/json"
        )
        return response
    should_fetch_tags_and_properties = type in [
        "entities",
        "models",
        "users",
        "providers",
    ]
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    row = db.query_resource(
        type, "name", "".join(resource), should_fetch_tags_and_properties
    )[0]

    if type == "features":
        data_as_list = collect_features(row)
    elif type == "training_sets":
        data_as_list = collect_training_sets(row)
    elif type == "sources":
        data_as_list = sources(row)  # todox: create other collect methods as needed
    elif type == "labels":
        data_as_list = labels(row)
    elif type == "entities":
        data_as_list = entities(row)
    elif type == "models":
        data_as_list = models(row)
    elif type == "users":
        data_as_list = users(row)
    elif type == "providers":
        data_as_list = providers(row)

    response = Response(
        response=json.dumps(data_as_list), status=200, mimetype="application/json"
    )
    return response


@dashboard_app.route("/data/search", methods=["GET"])
@cross_origin(allow_headers=["Content-Type"])
def SearchMetadata():
    raw_query = request.args["q"]
    rc = ResourceClient(local=True)
    results = rc.search(raw_query, True)
    payload = []
    for r in results:
        payload.append(
            {"Name": r["name"], "Variant": r["variant"], "Type": r["resource_type"]}
        )

    return json.dumps(payload)
