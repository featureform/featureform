from flask import Blueprint, Response
from flask_cors import CORS, cross_origin
import json
import importlib.resources as pkg_resources
from .sqlite_metadata import SQLiteMetadata
from .resources import SourceType
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
    ProviderResource)
import os
import sys

path = os.path.join(os.path.dirname(__file__), 'dashboard')

dashboard_app = Blueprint('dashboard_app', __name__, static_folder=path + '/out/', static_url_path='')

CORS(dashboard_app)
sqlObject = SQLiteMetadata() 

@dashboard_app.route('/')
def index():
    return dashboard_app.send_static_file('index.html')

@dashboard_app.route('/<type>')
def type(type):
    return dashboard_app.send_static_file('[type].html')

@dashboard_app.route('/<type>/<entity>')
def entity(type, entity):
    return dashboard_app.send_static_file('[type]/[entity].html')

@dashboard_app.route('/static/<asset>')
def deliver_static(asset):
    return dashboard_app.send_static_file('static/' + asset)

def variant_organiser(allVariantList):
    variantsDict = dict()

    for variant in allVariantList:
        name = variant["name"]
        if name in variantsDict: 
            variantsDict[name].append(variant)
        else:
            variantsDict[name] = [variant]
    
    return variantsDict

def feature_variant(variantData):
    variantsDict = dict()
    allVariantList = []
    variants = []
    
    for variantRow in variantData:
        featureVariant = FeatureVariantResource(
                variantRow['created'], 
                variantRow['description'], 
                variantRow['entity'], 
                variantRow['name'], 
                variantRow['owner'], 
                variantRow['provider'], 
                variantRow['data_type'], 
                variantRow['variant'], 
                variantRow['status'], 
                {"entity": variantRow['source_entity'],
                "value": variantRow['source_value'], 
                "timestamp": variantRow['source_timestamp']},
                {"Name":variantRow['source_name'],
                "Variant":variantRow['source_variant']} 
            ).toDictionary()

        allVariantList.append(variantRow['variant'])
        variantsDict[variantRow['variant']] = featureVariant
        variants.append(featureVariant)
    return variantsDict, allVariantList, variants

def features(featureRow):
    variantData = feature_variant(sqlObject.query_resource("feature_variant", "name",featureRow['name']))
    
    return FeatureResource(
                featureRow['name'], 
                featureRow['default_variant'], 
                "Feature", 
                variantData[0], 
                variantData[1] 
            ).toDictionary()

def training_set_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []
    for variantRow in variantData:
        try:
            feature_list = sqlObject.get_training_set_features(variantRow['name'], variantRow['variant'])
        except ValueError:
            feature_list = []

        trainingSetVariant = TrainingSetVariantResource(
                variantRow['created'], 
                variantRow['description'], 
                variantRow['name'], 
                variantRow['owner'], 
                variantRow['variant'], 
                {"Name":variantRow['label_name'],
                "Variant":variantRow['label_variant']}, 
                variantRow['status'], 
                variant_organiser(feature_variant(getTrainingSetFeatures(feature_list))[2]),
            ).toDictionary()
        allVariantList.append(variantRow['variant'])
        variantDict[variantRow['variant']] = trainingSetVariant
        variants.append(trainingSetVariant)

    return variantDict, allVariantList, variants
    
def getTrainingSetFeatures(feature_list):
    feature_variant_tuple = []
    for feature in feature_list:
        feature_variant_tuple.append(sqlObject.get_feature_variant(feature["feature_name"], feature["feature_variant"]))
    return feature_variant_tuple

def training_sets(rowData):
    variantData = training_set_variant(sqlObject.query_resource("training_set_variant", "name", rowData['name']))
    return TrainingSetResource( 
                "TrainingSet", 
                rowData['default_variant'], 
                rowData['name'], 
                variantData[0], 
                variantData[1] 
            ).toDictionary()

def source_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []
    for variantRow in variantData:
        try:
            feature_list = sqlObject.get_feature_variants_from_source(variantRow['name'], variantRow['variant'])
        except ValueError:
            feature_list = []
        training_set_list = set()
        for f in feature_list:
            try:
                features_training_set_list = sqlObject.get_training_set_from_features(f["name"], f["variant"])
                for training_set in features_training_set_list:
                    training_set_list.add(sqlObject.get_training_set_variant(training_set["training_set_name"], training_set["training_set_variant"]))
            except ValueError:
                continue
        try:
            label_list = sqlObject.get_label_variants_from_source(variantRow["name"], variantRow["variant"])
        except ValueError:
            label_list = []
        for l in label_list:
            try:
                labels_training_set_list = sqlObject.get_training_set_variant_from_label(l["name"], l["variant"])
                for training_set in labels_training_set_list:
                    training_set_list.add(training_set)
            except ValueError:
                continue
        if variantRow['transformation'] == SourceType.DF_TRANSFORMATION.value:
            definition = str(variantRow['definition'], 'latin-1')
        else:
            definition = variantRow['definition']
        sourceVariant = SourceVariantResource(
                variantRow['created'], 
                variantRow['description'], 
                variantRow['name'], 
                variantRow['source_type'], 
                variantRow['owner'], 
                variantRow['provider'], 
                variantRow['variant'], 
                variantRow['status'], 
                definition, 
                variant_organiser(label_variant(label_list)[2]), 
                variant_organiser(feature_variant(feature_list)[2]), 
                variant_organiser(training_set_variant(training_set_list)[2]) 
            ).toDictionary()
        allVariantList.append(variantRow['name'])
        variantDict[variantRow['variant']] = sourceVariant
        variants.append(sourceVariant)
    
    return variantDict, allVariantList, variants

def sources(rowData):
    variantData = source_variant(sqlObject.query_resource("source_variant", "name", rowData['name']))
    return SourceResource( 
                "Source", 
                rowData['default_variant'], 
                rowData['name'], 
                variantData[0], 
                variantData[1] 
            ).toDictionary()
    
def label_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []

    for variantRow in variantData:
        labelTuple = str((variantRow['name'], variantRow['variant']))
        labelVariant = LabelVariantResource(
                variantRow['created'], 
                variantRow['description'], 
                variantRow['entity'], 
                variantRow['name'], 
                variantRow['owner'], 
                variantRow['provider'], 
                variantRow['data_type'], 
                variantRow['variant'], 
                {"entity": variantRow['source_entity'],
                "value": variantRow['source_value'],
                "timestamp": variantRow['source_timestamp']},
                variantRow['status'], 
                {"Name":variantRow['source_name'],
                "Variant":variantRow['source_variant']}, 
                variant_organiser(training_set_variant(sqlObject.get_training_set_variant_from_label(variantRow['name'], variantRow['variant']))[2]) 
            ).toDictionary()
        
        allVariantList.append(variantRow['variant'])
        variantDict[variantRow['variant']] = labelVariant
        variants.append(labelVariant)
    return variantDict, allVariantList, variants

def labels(rowData):
    variantData = label_variant(sqlObject.query_resource("label_variant", "name", rowData['name']))
    return LabelResource(
                "Label", 
                rowData['default_variant'], 
                rowData['name'], 
                variantData[0], 
                variantData[1] 
            ).toDictionary()

def entities(rowData):
    label_list = sqlObject.query_resource( "label_variant", "entity", rowData['name'])
    training_set_list = set()
    for label in label_list:
        for training_set in sqlObject.get_training_set_variant_from_label(label["name"], label["variant"]):
            training_set_list.add(training_set)
    return EntityResource(
                rowData['name'], 
                rowData['type'], 
                rowData['description'], 
                rowData['status'], 
                variant_organiser(feature_variant(sqlObject.query_resource( "feature_variant", "entity", rowData['name']))[2]), 
                variant_organiser(label_variant(label_list)[2]), 
                variant_organiser(training_set_variant(training_set_list)[2]) 
            ).toDictionary()

def models(rowData):
    return ModelResource(
                rowData['name'], 
                "Model", 
                rowData['description'], 
                rowData['status'], 
                variant_organiser(feature_variant(sqlObject.query_resource( "feature_variant", "name", rowData['name']))[2]), 
                variant_organiser(label_variant(sqlObject.query_resource( "label_variant", "variant", rowData['name']))[2]), 
                variant_organiser(training_set_variant(sqlObject.query_resource( "training_set_variant", "name", rowData['name']))[2]) 
            ).toDictionary()

def users(rowData):
    return UserResource(
                rowData['name'], 
                rowData['type'], 
                rowData['status'],  
                variant_organiser(feature_variant(sqlObject.query_resource( "feature_variant", "owner", rowData['name']))[2]), 
                variant_organiser(label_variant(sqlObject.query_resource( "label_variant", "owner", rowData['name']))[2]), 
                variant_organiser(training_set_variant(sqlObject.query_resource( "training_set_variant", "owner", rowData['name']))[2]), 
                variant_organiser(source_variant(sqlObject.query_resource( "source_variant", "owner", rowData['name']))[2]), 
            ).toDictionary()

def providers(rowData):
    try:
        source_list = sqlObject.query_resource( "source_variant", "provider", rowData['name'])
    except ValueError:
        source_list = []
    try:
        feature_list = sqlObject.query_resource( "feature_variant", "provider", rowData['name'])
    except ValueError:
        feature_list = []
    try:
        label_list = sqlObject.query_resource( "label_variant", "provider", rowData['name'])
    except ValueError:
        label_list = []
    return ProviderResource(
                rowData['name'], 
                rowData['type'], 
                rowData['description'], 
                rowData['provider_type'], 
                rowData['software'], 
                rowData['team'], 
                variant_organiser(source_variant(source_list)[2]), 
                rowData['status'], 
                rowData['serialized_config'],
                variant_organiser(feature_variant(feature_list)[2]), 
                variant_organiser(label_variant(label_list)[2])
            ).toDictionary()

@dashboard_app.route("/data/<type>", methods = ['POST', 'GET'])
@cross_origin(allow_headers=['Content-Type'])
def GetMetadataList(type):
    type = type.replace("-", "_")
    if type not in ["features", "training_sets", "sources", "labels", "entities", "models", "users", "providers"]:
        errorData = f"invalid resource type: {type}"
        response = Response(
        response=json.dumps(errorData),
        status=400,
        mimetype='application/json'
        )
        return response
    tableDataCursor = sqlObject.get_type_table(type)
    allData = []
    for row in tableDataCursor:
        if type == "features":
            allData.append(features(row))
        elif type == "training_sets":
            allData.append(training_sets(row))
        elif type == "sources":
            allData.append(sources(row))
        elif type == "labels":
            allData.append(labels(row))
        elif type == "entities":
            allData.append(entities(row))
        elif type == "models":
            allData.append(models(row))
        elif type == "users":
            allData.append(users(row))
        elif type == "providers":
            allData.append(providers(row))

    response = Response(
        response=json.dumps(allData),
        status=200,
        mimetype='application/json'
    )
    return response

@dashboard_app.route("/data/<type>/<resource>", methods = ['POST', 'GET'])
@cross_origin(allow_headers=['Content-Type'])
def GetMetadata(type, resource):
        type = type.replace("-", "_")
        if type not in ["features", "training_sets", "sources", "labels", "entities", "models", "users", "providers"]:
            errorData = f"invalid resource type: {type}"
            response = Response(
            response=json.dumps(errorData),
            status=400,
            mimetype='application/json'
            )
            return response
        row = sqlObject.query_resource(type, "name", "".join(resource))[0]

        if type == "features":
            dataAsList =  features(row)
        elif type == "training_sets":
            dataAsList =  training_sets(row)
        elif type == "sources":
            dataAsList =  sources(row)
        elif type == "labels":
            dataAsList =  labels(row)
        elif type == "entities":
            dataAsList =  entities(row)
        elif type == "models":
            dataAsList =  models(row)
        elif type == "users":
            dataAsList =  users(row)
        elif type == "providers":
            dataAsList =  providers(row)

        response = Response(
            response=json.dumps(dataAsList),
            status=200,
            mimetype='application/json'
        )
        return response
        