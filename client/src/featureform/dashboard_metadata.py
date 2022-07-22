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
sql_object = SQLiteMetadata() 

def variant_organiser(all_variant_list):
    variants_dict = dict()

    for variant in all_variant_list:
        name = variant["name"]
        if name in variants_dict: #Check if the key exists
            variants_dict[name].append(variant)
        else:
            variants_dict[name] = [variant]
    
    return variants_dict

def feature_variant(variant_data):
    variants_dict = dict()
    all_variant_list = []
    variants = []
    
    for variant_row in variant_data:
        featureVariant = FeatureVariantResource(
                variant_row['created'], #created
                variant_row['description'], #description
                variant_row['entity'], #entity 
                variant_row['featureName'], #featureName
                variant_row['owner'], #owner
                variant_row['provider'], #provider
                variant_row['dataType'], #dataType
                variant_row['variantName'], #variantName
                variant_row['status'], #status
                {"entity": variant_row['sourceEntity'],
                "value": variant_row['sourceValue'], #location
                "timestamp": variant_row['sourceTimestamp']},
                {"Name":variant_row['sourceName'],
                "Variant":variant_row['sourceVariant']} #source
            ).toDictionary()

        all_variant_list.append(variant_row['variantName'])
        variants_dict[variant_row['variantName']] = featureVariant
        variants.append(featureVariant)
    return variants_dict, all_variant_list, variants

def features(featureRow):
    variantData = feature_variant(sql_object.get_variant_resource("feature_variant", "featureName",featureRow['name']))
    #Return an object of the row
    return FeatureResource(
                featureRow['name'], #name
                featureRow['defaultVariant'], #defaultVariant
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
                variantRow['trainingSetName'], #trainingSetName
                variantRow['owner'], #owner
                variantRow['variantName'], #variantName
                {"Name":variantRow['labelName'],
                "Variant":variantRow['labelVariant']}, #label
                variantRow['status'], #status
                variant_organiser(feature_variant(getTrainingSetFeatures(variantRow['features']))[2]),
            ).toDictionary()
        allVariantList.append(variantRow['variantName'])
        variantDict[variantRow['variantName']] = trainingSetVariant
        variants.append(trainingSetVariant)

    return variantDict, allVariantList, variants
    
def getTrainingSetFeatures(feature_list):
    # parse the featureList str(list of tuples)
    featureVariantTuple = []
    features = [tuple(feature_tuple.split('\', \'')) for feature_tuple in re.findall("\(\'(.*?)\'\)", feature_list)]
    # Iterate throigh the list and get one large tuple of tuples
    for feature in features:
        featureVariantTuple += sql_object.get_name_variant("feature_variant", "featureName", feature[0], "variantName", feature[1])
    
    return featureVariantTuple

def training_sets(row_data):
    variant_data = training_set_variant(sql_object.get_variant_resource("training_set_variant", "trainingSetName", row_data['name']))
    return TrainingSetResource( 
                row_data['type'], #type
                row_data['defaultVariant'], #defaultvariant
                row_data['name'], #name
                variant_data[0], #variantsDict
                variant_data[1] #all variants
            ).toDictionary()

def source_variant(variant_data):
    variant_dict = dict()
    all_variant_list = []
    variants = []
    for variant_row in variant_data:
        source_variant = SourceVariantResource(
                
                variant_row['created'], #created
                variant_row['description'], #description
                variant_row['name'], #sourceName
                variant_row['sourceType'], #sourceType
                variant_row['owner'], #owner
                variant_row['provider'], #provider
                variant_row['variant'], #variant
                variant_row['status'], #status
                variant_row['definition'], #definition
                variant_organiser(label_variant(sql_object.get_name_variant( "labels_variant", "sourceName", variant_row['name'], "sourceVariant", variant_row['variant']))[2]), #labels
                variant_organiser(feature_variant(sql_object.get_name_variant( "feature_variant", "sourceName", variant_row['name'],"sourceVariant",variant_row['variant']))[2]), #features
                variant_organiser(training_set_variant(sql_object.get_variant_resource( "training_set_variant", "variantName", variant_row['variant']))[2]) #training sets
            ).toDictionary()
        all_variant_list.append(variant_row['name'])
        variant_dict[variant_row['variant']] = source_variant
        variants.append(source_variant)
    
    return variant_dict, all_variant_list, variants

def sources(row_data):
    variantData = source_variant(sql_object.get_variant_resource("source_variant", "sourceName", row_data['name']))
    return SourceResource( 
                row_data['type'], #type
                row_data['defaultVariant'], #defaultVariant
                #source_variant(sqlObject.get_name_variant("source_variant", "sourceName", rowData[2], "variantName", rowData[1]))[0], #defaultvariant
                row_data['name'], #name
                variantData[0], #variants
                variantData[1] #all variants
            ).toDictionary()
    
def label_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []

    for variantRow in variantData:
        labelTuple = str((variantRow['labelName'], variantRow['variantName']))
        labelVariant = LabelVariantResource(
                variantRow['created'], #created
                variantRow['description'], #description
                variantRow['entity'], #entity
                variantRow['labelName'], #labelName
                variantRow['owner'], #owner
                variantRow['provider'], #provider
                variantRow['dataType'], #dataType
                variantRow['variantName'], #variantName
                {"entity": variantRow['sourceEntity'],
                "value": variantRow['sourceValue'],
                "timestamp": variantRow['sourceTimestamp']},
                variantRow['status'], #status
                {"Name":variantRow['sourceName'],
                "Variant":variantRow['sourceVariant']}, #source
                variant_organiser(training_set_variant(sql_object.get_variant_resource( "training_set_variant", "label", labelTuple))[2]) #training sets
            ).toDictionary()
        
        allVariantList.append(variantRow['variantName'])
        variantDict[variantRow['variantName']] = labelVariant
        variants.append(labelVariant)
    return variantDict, allVariantList, variants

def labels(rowData):
    variantData = label_variant(sql_object.get_variant_resource("labels_variant", "labelName", rowData['name']))
    return LabelResource(
                
                rowData['type'], #type
                rowData['defaultVariant'], #defaultvariant
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
                variant_organiser(feature_variant(sql_object.get_variant_resource( "feature_variant", "entity", rowData['name']))[2]), #features
                variant_organiser(label_variant(sql_object.get_variant_resource( "labels_variant", "entity", rowData['name']))[2]), #labels
                variant_organiser(training_set_variant(sql_object.get_variant_resource( "training_set_variant", "label", rowData['name']))[2]) #training sets
            ).toDictionary()

def models(rowData):
    return ModelResource(
                rowData['name'], #name
                rowData['type'], #type
                rowData['description'], #description
                rowData['status'], #status
                variant_organiser(feature_variant(sql_object.get_variant_resource( "feature_variant", "featureName", rowData['name']))[2]), #features
                variant_organiser(label_variant(sql_object.get_variant_resource( "labels_variant", "variantName ", rowData['name']))[2]), #labels
                variant_organiser(training_set_variant(sql_object.get_variant_resource( "training_set_variant", "trainingSetName", rowData['name']))[2]) #training sets
            ).toDictionary()

def users(rowData):
    return UserResource(
                rowData['name'], #name
                rowData['type'], #type
                rowData['status'],  #status
                variant_organiser(feature_variant(sql_object.get_variant_resource( "feature_variant", "owner", rowData['name']))[2]), #features
                variant_organiser(label_variant(sql_object.get_variant_resource( "labels_variant", "owner", rowData['name']))[2]), #labels
                variant_organiser(training_set_variant(sql_object.get_variant_resource( "training_set_variant", "owner", rowData['name']))[2]), #training sets
                variant_organiser(source_variant(sql_object.get_variant_resource( "source_variant", "owner", rowData['name']))[2]), #training sets
            ).toDictionary()

def providers(rowData):
    return ProviderResource(
                
                rowData['name'], #name
                rowData['type'], #type
                rowData['description'], #description
                rowData['providerType'], #provider type
                rowData['software'], #software
                rowData['team'], #team
                variant_organiser(source_variant(sql_object.get_variant_resource( "source_variant", "provider", rowData['name']))[2]), #sources
                rowData['status'], #status
                rowData['serializedConfig'],#serialis...
                variant_organiser(feature_variant(sql_object.get_variant_resource( "feature_variant", "provider", rowData['name']))[2]), #features
                variant_organiser(label_variant(sql_object.get_variant_resource( "labels_variant", "provider", rowData['name']))[2]), #labels
                #variant_organiser(training_set_variant(sqlObject.get_variant_resource( "training_set_variant", "provider", rowData[0]))[2]), #training sets
            ).toDictionary()

@app.route("/data/<type>", methods = ['POST', 'GET'])
@cross_origin(allow_headers=['Content-Type'])
def GetMetadataList(type):
    type = type.replace("-", "_")
    tableDataCursor = sql_object.get_type_table(type)
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
        row = sql_object.get_variant_resource(type, "name", "".join(resource))[0]

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
