from flask import Flask
from flask_cors import CORS, cross_origin
import json
import re
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
    ProviderResource)

app = Flask(__name__)
CORS(app)
sqlObject = SQLiteMetadata() 

def variant_organiser(allVariantList):
    variantsDict = dict()

    for variant in allVariantList:
        name = variant["name"]
        if name in variantsDict: #Check if the key exists
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
                variantRow['created'], #created
                variantRow['description'], #description
                variantRow['entity'], #entity 
                variantRow['name'], #featureName
                variantRow['owner'], #owner
                variantRow['provider'], #provider
                variantRow['data_type'], #dataType
                variantRow['variant'], #variantName
                variantRow['status'], #status
                {"entity": variantRow['source_entity'],
                "value": variantRow['source_value'], #location
                "timestamp": variantRow['source_timestamp']},
                {"Name":variantRow['source_name'],
                "Variant":variantRow['source_variant']} #source
            ).toDictionary()

        allVariantList.append(variantRow['variant'])
        variantsDict[variantRow['variant']] = featureVariant
        variants.append(featureVariant)
    return variantsDict, allVariantList, variants

def features(featureRow):
    variantData = feature_variant(sqlObject.getVariantResource("feature_variant", "name",featureRow['name']))
    #Return an object of the row
    return FeatureResource(
                featureRow['name'], #name
                featureRow['default_variant'], #defaultVariant
                featureRow['type'], #type
                variantData[0], #variantsDict
                variantData[1] #All Variants
            ).toDictionary()

def training_set_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []

    for variantRow in variantData:
        trainingSetVariant = TrainingSetVariantResource(
                
                variantRow['created'], #created
                variantRow['description'], #description  
                variantRow['name'], #training set name
                variantRow['owner'], #owner
                variantRow['variant'], #variantName
                {"Name":variantRow['label_name'],
                "Variant":variantRow['label_variant']}, #label
                variantRow['status'], #status
                variant_organiser(feature_variant(getTrainingSetFeatures(variantRow['features']))[2]),
            ).toDictionary()
        allVariantList.append(variantRow['variant'])
        variantDict[variantRow['variant']] = trainingSetVariant
        variants.append(trainingSetVariant)

    return variantDict, allVariantList, variants
    
def getTrainingSetFeatures(featureList):
    featureVariantTuple = []
    features = [tuple(featureTuple.split('\', \'')) for featureTuple in re.findall("\(\'(.*?)\'\)", featureList)]
    for feature in features:
        featureVariantTuple += sqlObject.getNameVariant("feature_variant", "name", feature[0], "variant", feature[1])
    
    return featureVariantTuple

def training_sets(rowData):
    variantData = training_set_variant(sqlObject.getVariantResource("training_set_variant", "name", rowData['name']))
    return TrainingSetResource( 
                rowData['type'], #type
                rowData['default_variant'], #defaultvariant
                rowData['name'], #name
                variantData[0], #variantsDict
                variantData[1] #all variants
            ).toDictionary()

def source_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []
    for variantRow in variantData:
        sourceVariant = SourceVariantResource(
                
                variantRow['created'], #created
                variantRow['description'], #description
                variantRow['name'], #sourceName
                variantRow['source_type'], #sourceType
                variantRow['owner'], #owner
                variantRow['provider'], #provider
                variantRow['variant'], #variant
                variantRow['status'], #status
                variantRow['definition'], #definition
                variant_organiser(label_variant(sqlObject.getNameVariant( "label_variant", "source_name", variantRow['name'], "source_variant", variantRow['variant']))[2]), #labels
                variant_organiser(feature_variant(sqlObject.getNameVariant( "feature_variant", "source_name", variantRow['name'],"source_variant",variantRow['variant']))[2]), #features
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "variant", variantRow['variant']))[2]) #training sets
            ).toDictionary()
        allVariantList.append(variantRow['name'])
        variantDict[variantRow['variant']] = sourceVariant
        variants.append(sourceVariant)
    
    return variantDict, allVariantList, variants

def sources(rowData):
    variantData = source_variant(sqlObject.getVariantResource("source_variant", "name", rowData['name']))
    return SourceResource( 
                rowData['type'], #type
                rowData['default_variant'], #defaultVariant
                rowData['name'], #name
                variantData[0], #variants
                variantData[1] #all variants
            ).toDictionary()
    
def label_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []

    for variantRow in variantData:
        labelTuple = str((variantRow['name'], variantRow['variant']))
        labelVariant = LabelVariantResource(
                variantRow['created'], #created
                variantRow['description'], #description
                variantRow['entity'], #entity
                variantRow['name'], #labelName
                variantRow['owner'], #owner
                variantRow['provider'], #provider
                variantRow['data_type'], #dataType
                variantRow['variant'], #variantName
                {"entity": variantRow['source_entity'],
                "value": variantRow['source_value'],
                "timestamp": variantRow['source_timestamp']},
                variantRow['status'], #status
                {"Name":variantRow['source_name'],
                "Variant":variantRow['source_variant']}, #source
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "label", labelTuple))[2]) #training sets
            ).toDictionary()
        
        allVariantList.append(variantRow['variant'])
        variantDict[variantRow['variant']] = labelVariant
        variants.append(labelVariant)
    return variantDict, allVariantList, variants

def labels(rowData):
    variantData = label_variant(sqlObject.getVariantResource("label_variant", "name", rowData['name']))
    return LabelResource(
                
                rowData['type'], #type
                rowData['default_variant'], #defaultvariant
                rowData['name'], #name
                variantData[0], #variantDict
                variantData[1] #allVariants
            ).toDictionary()

def entities(rowData):
    return EntityResource(
                
                rowData['name'], #name
                rowData['type'], #type
                rowData['description'], #description
                rowData['status'], #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "entity", rowData['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "label_variant", "entity", rowData['name']))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "label", rowData['name']))[2]) #training sets
            ).toDictionary()

def models(rowData):
    return ModelResource(
                rowData['name'], #name
                rowData['type'], #type
                rowData['description'], #description
                rowData['status'], #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "name", rowData['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "label_variant", "variant", rowData['name']))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "name", rowData['name']))[2]) #training sets
            ).toDictionary()

def users(rowData):
    return UserResource(
                rowData['name'], #name
                rowData['type'], #type
                rowData['status'],  #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "owner", rowData['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "label_variant", "owner", rowData['name']))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "owner", rowData['name']))[2]), #training sets
                variant_organiser(source_variant(sqlObject.getVariantResource( "source_variant", "owner", rowData['name']))[2]), #training sets
            ).toDictionary()

def providers(rowData):
    return ProviderResource(
                rowData['name'], #name
                rowData['type'], #type
                rowData['description'], #description
                rowData['provider_type'], #provider type
                rowData['software'], #software
                rowData['team'], #team
                variant_organiser(source_variant(sqlObject.getVariantResource( "source_variant", "provider", rowData['name']))[2]), #sources
                rowData['status'], #status
                rowData['serialized_config'],#serialis...
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "provider", rowData['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "label_variant", "provider", rowData['name']))[2]), #labels
                #variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "provider", rowData[0]))[2]), #training sets
            ).toDictionary()

@app.route("/data/<type>", methods = ['POST', 'GET'])
@cross_origin(allow_headers=['Content-Type'])
def GetMetadataList(type):
    type = type.replace("-", "_")
    tableDataCursor = sqlObject.getTypeTable(type)
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
        else:
            allData.append("INCORRECT TYPE")

    response = app.response_class(
        response=json.dumps(allData),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route("/data/<type>/<resource>", methods = ['POST', 'GET'])
@cross_origin(allow_headers=['Content-Type'])
def GetMetadata(type, resource):
        type = type.replace("-", "_")
        row = sqlObject.getVariantResource(type, "name", "".join(resource))[0]

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
        else:
            dataAsList = "INCORRECT TYPE"

        response = app.response_class(
            response=json.dumps(dataAsList),
            status=200,
            mimetype='application/json'
        )
        return response
