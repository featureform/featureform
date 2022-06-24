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
                variantRow[0], #created
                variantRow[1], #description
                variantRow[2], #entity 
                variantRow[3], #featureName
                variantRow[4], #owner
                variantRow[5], #provider
                variantRow[6], #dataType
                variantRow[7], #variantName
                variantRow[8], #status
                {"entity": variantRow[9],
                "value": variantRow[11], #location
                "timestamp": variantRow[10]},
                {"Name":variantRow[12],
                "Variant":variantRow[13]} #source
            ).toDictionary()

        allVariantList.append(variantRow[7])
        variantsDict[variantRow[7]] = featureVariant
        variants.append(featureVariant)
    return variantsDict, allVariantList, variants

def features(featureRow):
    variantData = feature_variant(sqlObject.getVariantResource("feature_variant", "featureName",featureRow[0]))
    #Return an object of the row
    return FeatureResource(
                featureRow[0], #name
                featureRow[1], #defaultVariant
                featureRow[2], #type
                variantData[0], #variantsDict
                variantData[1] #All Variants
            ).toDictionary()

def training_set_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []

    for variantRow in variantData:
        trainingSetVariant = TrainingSetVariantResource(
                
                variantRow[0], #created
                variantRow[1], #description  
                variantRow[2], #trainingSetName
                variantRow[3], #owner
                variantRow[4], #variantName
                variantRow[5], #label
                variantRow[6], #status
                variant_organiser(feature_variant(getTrainingSetFeatures(variantRow[7]))[2]),
            ).toDictionary()
        allVariantList.append(variantRow[4])
        variantDict[variantRow[4]] = trainingSetVariant
        variants.append(trainingSetVariant)

    return variantDict, allVariantList, variants
    
def getTrainingSetFeatures(featureList):
    # parse the featureList str(list of tuples)
    featureVariantTuple = []
    features = [tuple(featureTuple.split('\', \'')) for featureTuple in re.findall("\(\'(.*?)\'\)", featureList)]
    # Iterate throigh the list and get one large tuple of tuples
    for feature in features:
        featureVariantTuple += sqlObject.getNameVariant("feature_variant", "featureName", feature[0], "variantName", feature[1])
    
    return featureVariantTuple

def training_sets(rowData):
    variantData = training_set_variant(sqlObject.getVariantResource("training_set_variant", "trainingSetName", rowData[2]))
    return TrainingSetResource( 
                rowData[0], #type
                rowData[1], #defaultvariant
                rowData[2], #name
                variantData[0], #variantsDict
                variantData[1] #all variants
            ).toDictionary()

def source_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []
    for variantRow in variantData:
        sourceVariant = SourceVariantResource(
                
                variantRow[0], #created
                variantRow[1], #description
                variantRow[2], #sourceName
                variantRow[3], #sourceType
                variantRow[4], #owner
                variantRow[5], #provider
                variantRow[6], #variant
                variantRow[7], #status
                variantRow[8], #definition
                variant_organiser(label_variant(sqlObject.getNameVariant( "labels_variant", "sourceName", variantRow[2], "sourceVariant", variantRow[6]))[2]), #labels
                variant_organiser(feature_variant(sqlObject.getNameVariant( "feature_variant", "sourceName", variantRow[2],"sourceVariant",variantRow[6]))[2]), #features
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "variantName", variantRow[6]))[2]) #training sets
            ).toDictionary()
        allVariantList.append(variantRow[2])
        variantDict[variantRow[6]] = sourceVariant
        variants.append(sourceVariant)
    
    return variantDict, allVariantList, variants

def sources(rowData):
    variantData = source_variant(sqlObject.getVariantResource("source_variant", "sourceName", rowData[2]))
    return SourceResource( 
                rowData[0], #type
                rowData[1], #defaultVariant
                #source_variant(sqlObject.getNameVariant("source_variant", "sourceName", rowData[2], "variantName", rowData[1]))[0], #defaultvariant
                rowData[2], #name
                variantData[0], #variants
                variantData[1] #all variants
            ).toDictionary()
    
def label_variant(variantData):
    variantDict = dict()
    allVariantList = []
    variants = []

    for variantRow in variantData:
        labelTuple = str((variantRow[3], variantRow[8]))
        labelVariant = LabelVariantResource(
                variantRow[0], #created
                variantRow[1], #description
                variantRow[2], #entity
                variantRow[3], #labelName
                variantRow[4], #owner
                variantRow[6], #provider
                variantRow[7], #dataType
                variantRow[8], #variantName
                {"entity": variantRow[8],
                "value": variantRow[9],
                "timestamp": variantRow[10]},
                variantRow[11], #status
                {"Name":variantRow[12],
                "Variant":variantRow[13]}, #source
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "label", labelTuple))[2]) #training sets
            ).toDictionary()
        
        allVariantList.append(variantRow[7])
        variantDict[variantRow[7]] = labelVariant
        variants.append(labelVariant)
    return variantDict, allVariantList, variants

def labels(rowData):
    variantData = label_variant(sqlObject.getVariantResource("labels_variant", "labelName", rowData[2]))
    return LabelResource(
                
                rowData[0], #type
                rowData[1], #defaultvariant
                rowData[2], #name
                variantData[0], #variantDict
                variantData[1] #allVariants
            ).toDictionary()

def entities(rowData):
    return EntityResource(
                
                rowData[0], #name
                rowData[1], #type
                rowData[2], #description
                rowData[3], #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "entity", rowData[0]))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "entity", rowData[0]))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "label", rowData[0]))[2]) #training sets
            ).toDictionary()

def models(rowData):
    return ModelResource(
                rowData[0], #name
                rowData[1], #type
                rowData[2], #description
                rowData[3], #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "featureName", rowData[0]))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "variantName ", rowData[0]))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "trainingSetName", rowData[0]))[2]) #training sets
            ).toDictionary()

def users(rowData):
    return UserResource(
                rowData[0], #name
                rowData[1], #type
                rowData[2],  #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "owner", rowData[0]))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "owner", rowData[0]))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "owner", rowData[0]))[2]), #training sets
                variant_organiser(source_variant(sqlObject.getVariantResource( "source_variant", "owner", rowData[0]))[2]), #training sets
            ).toDictionary()

def providers(rowData):
    return ProviderResource(
                
                rowData[0], #name
                rowData[1], #type
                rowData[2], #description
                rowData[3], #provider type
                rowData[4], #software
                rowData[5], #team
                variant_organiser(source_variant(sqlObject.getVariantResource( "source_variant", "provider", rowData[0]))[2]), #sources
                rowData[7], #status
                rowData[8],#serialis...
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "provider", rowData[0]))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "provider", rowData[0]))[2]), #labels
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
