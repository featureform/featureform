import json
import os

import pandas as pd
from featureform import ResourceClient
from featureform.serving import LocalClientImpl
from flask import Blueprint, Response, request
from flask_cors import CORS, cross_origin
from .metadata_repository import MetadataRepositoryLocalImpl, Feature, FeatureVariant
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
        error = f"Error 400: GetSourceData - Could not find the name({name}) or variant({variant}) query parameters"
        response = Response(
            response=json.dumps(error), status=400, mimetype="application/json"
        )
        return response

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


def variant_organiser(allVariantList):
    variantsDict = dict()

    for variant in allVariantList:
        name = variant["name"]
        if name in variantsDict:
            variantsDict[name].append(variant)
        else:
            variantsDict[name] = [variant]

    return variantsDict


def feature_variant(variantData: FeatureVariant):
    variantsDict = dict()
    allVariantList = []
    variants = []

    for variantRow in variantData:
        featureVariant = FeatureVariantResource(
            variantRow["created"],
            variantRow["description"],
            variantRow["entity"],
            variantRow["name"],
            variantRow["owner"],
            variantRow["provider"],
            variantRow["data_type"],
            variantRow["variant"],
            variantRow["status"],
            {
                "entity": variantRow["source_entity"],
                "value": variantRow["source_value"],
                "timestamp": variantRow["source_timestamp"],
            },
            {
                "Name": variantRow["source_name"],
                "Variant": variantRow["source_variant"],
            },
            json.loads(variantRow["tags"]) if variantRow["tags"] is not None else [],
            json.loads(variantRow["properties"])
            if variantRow["properties"] is not None
            else {},
        ).toDictionary()

        allVariantList.append(variantRow["variant"])
        variantsDict[variantRow["variant"]] = featureVariant
        variants.append(featureVariant)
    return variantsDict, allVariantList, variants


def collect_features(feature: Feature):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    variantList = []
    for variantName in feature["variants"]:
        found_variant = db.get_feature_variant(
            name=feature["name"], variant=variantName
        )
        variantList.append(feature_variant(found_variant))

    return FeatureResource(
        name=feature["name"],
        defaultVariant=feature["default_variant"],
        type="Feature",
        allVariants=feature["variants"],
        variants=variantList,
    ).toDictionary()


def training_set_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []
    for variantRow in variantData:
        try:
            db = MetadataRepositoryLocalImpl(SQLiteMetadata())
            # todox: replace db method
            feature_list = db.get_training_set_variant(
                variantRow["name"], variantRow["variant"]
            )
        except ValueError:
            feature_list = []

        trainingSetVariant = TrainingSetVariantResource(
            variantRow["created"],
            variantRow["description"],
            variantRow["name"],
            variantRow["owner"],
            variantRow["variant"],
            {"Name": variantRow["label_name"], "Variant": variantRow["label_variant"]},
            variantRow["status"],
            variant_organiser(feature_variant(getTrainingSetFeatures(feature_list))[2]),
            json.loads(variantRow["tags"]) if variantRow["tags"] is not None else [],
            json.loads(variantRow["properties"])
            if variantRow["properties"] is not None
            else {},
        ).toDictionary()
        allVariantList.append(variantRow["variant"])
        variantDict[variantRow["variant"]] = trainingSetVariant
        variants.append(trainingSetVariant)

    return variantDict, allVariantList, variants


def getTrainingSetFeatures(feature_list):
    feature_variant_tuple = []
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    for feature in feature_list:
        feature_variant_tuple.append(
            db.get_feature_variant(feature["feature_name"], feature["feature_variant"])
        )
    return feature_variant_tuple


def training_sets(rowData):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    variantData = training_set_variant(
        db.get_training_set_variant(rowData["name"], rowData["variant"])
    )
    return TrainingSetResource(
        "TrainingSet",
        rowData["default_variant"],
        rowData["name"],
        variantData[0],
        variantData[1],
    ).toDictionary()


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
            ).toDictionary()
            allVariantList.append(variantRow["variant"])
            variantDict[variantRow["variant"]] = sourceVariant
            variants.append(sourceVariant)

        return variantDict, allVariantList, variants


def sources(rowData):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method. do we need a get resource_variant list?
    source_list = db.get_sources()
    found_source = source_list.filter(
        lambda x: x["name"] == rowData["name"], source_list
    )
    # todox: what do do about this guy?
    return SourceResource(
        "Source",
        rowData["default_variant"],
        rowData["name"],
        found_source.variants,
        found_source.variants,  # todox: how do we handle all-variants vs the resource variant list?
    ).toDictionary()


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
        ).toDictionary()

        allVariantList.append(variantRow["variant"])
        variantDict[variantRow["variant"]] = labelVariant
        variants.append(labelVariant)
        return variantDict, allVariantList, variants


def labels(rowData):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    label_list = db.get_labels()
    found_variant = label_list.filter(
        lambda x: x["name"] == rowData["name"] and x["variant"] == rowData["variant"],
        label_list,
    )
    return LabelResource(
        "Label",
        rowData["default_variant"],
        rowData["name"],
        found_variant.variant,
        found_variant.variant,  # todox: same issue as before, need to map the previous all-variants & variants with the new repo return
    ).toDictionary()


def entities(rowData):
    db = MetadataRepositoryLocalImpl(SQLiteMetadata())
    # todox: replace db method
    try:
        label_list = db.get_labels()
        found_variant = label_list.filter(
            lambda x: x["name"] == rowData["name"]
            and x["variant"] == rowData["variant"],
            label_list,
        )
    except Exception as e:
        print(f"No labels found for entity {rowData['name']} - {e}")
        label_list = []
    training_set_list = set()
    for label in label_list:
        for training_set in db.get_training_set_variant_from_label(
            label["name"], label["variant"]
        ):
            training_set_list.add(training_set)
    return EntityResource(
        rowData["name"],
        rowData["type"],
        rowData["description"],
        rowData["status"],
        variant_organiser(
            feature_variant(
                db.query_resource_variant("feature_variant", "entity", rowData["name"])
            )[2]
        ),
        variant_organiser(label_variant(label_list)[2]),
        variant_organiser(training_set_variant(training_set_list)[2]),
        json.loads(rowData["tags"]) if rowData["tags"] is not None else [],
        json.loads(rowData["properties"]) if rowData["properties"] is not None else {},
    ).toDictionary()


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
    ).toDictionary()


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
    ).toDictionary()


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
    ).toDictionary()


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
        dataAsList = features(row)
    elif type == "training_sets":
        dataAsList = training_sets(row)
    elif type == "sources":
        dataAsList = sources(row)
    elif type == "labels":
        dataAsList = labels(row)
    elif type == "entities":
        dataAsList = entities(row)
    elif type == "models":
        dataAsList = models(row)
    elif type == "users":
        dataAsList = users(row)
    elif type == "providers":
        dataAsList = providers(row)

    response = Response(
        response=json.dumps(dataAsList), status=200, mimetype="application/json"
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
