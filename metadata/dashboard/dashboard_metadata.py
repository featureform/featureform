from lib2to3.pytree import type_repr
import logging
import resource
from flask import Flask
import json
from type_objects import (
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
from sqlLite import SQLiteTest

app = Flask(__name__)
sqlObject = SQLiteTest() 

# This function is complete except for the training set variable

def feature_variant(variantData):
    #Store each row's variant data in a map
    variantsDict = dict()
    allVariantList = []
    for variantRow in variantData:
        print(variantRow)
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
                "value": variantRow[11],
                "timestamp": variantRow[10]},
                variantRow[12], #source
            ).dictionary
        allVariantList.append(variantRow[7])
        variantsDict[variantRow[7]] = featureVariant
    return allVariantList, variantsDict

def features(featureRow):
    variantData = feature_variant(sqlObject.getVariantResource("feature_variant", "featureName",featureRow[0]))
    #Return an object of the row
    return FeatureResource(
                featureRow[0], #name
                featureRow[1], #defaultVariant
                featureRow[2], #type
                variantData[1], #variantsDict
                variantData[0] #All Variants
            )

def training_set_variant(variantData):
    variantDict = dict()
    allVariantList = []
    for variantRow in variantData:
        trainingSetVariant = TrainingSetVariantResource(
                
                variantRow[0], #created
                variantRow[1], #descrition  
                variantRow[2], #trainingSetName
                variantRow[3], #owner
                variantRow[4], #provider
                variantRow[5], #variantName
                variantRow[6], #entity
                variantRow[7], #label
                variantRow[8],  #status
                feature_variant(sqlObject.getVariantResource("features_variant", "variantName", variantRow[5]))[1]
            )
        allVariantList.append(variantRow[5])
        variantDict[variantRow[5]] = trainingSetVariant

    return variantDict, allVariantList

def training_sets(rowData):
    variantData = training_set_variant(sqlObject.getVariantResource("training_set_variant", "trainingSetName", rowData[2]))
    return TrainingSetResource( 
                rowData[0], #type
                rowData[1], #defaultvariant
                rowData[2], #name
                variantData[0], #variantsDict
                variantData[1] #all variants
            )

def source_variant(variantData):
    variantDict = dict()
    allVariantList = []
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
                label_variant(sqlObject.getVariantResource( "labels_variant", "sourceEntity", variantRow[2]))[0], #labels
                feature_variant(sqlObject.getVariantResource( "features_variant", "variantName", variantRow[6]))[1], #features
                training_set_variant(sqlObject.getVariantResource( "training_set_variant", "variantName", variantRow[6]))[0] #training sets
            )
        allVariantList.append(variantRow[2])
        variantDict[variantRow[6]] = sourceVariant
    return variantDict, allVariantList

def sources(rowData):
    variantData = source_variant(sqlObject.getVariantResource("sources", "sourceName", rowData[2]))
    return SourceResource( 
                rowData[0], #type
                rowData[1], #defaultvariant
                rowData[2], #name
                variantData[0], #variants
                variantData[1] #all variants
            )

# make the variables like "features()". remove json
def label_variant(variantData):
    variantDict = dict()
    allVariantList = []
    for variantRow in variantData:
        labelVariant = LabelVariantResource(
                variantRow[0], #created
                variantRow[1], #description
                variantRow[2], #entity
                variantRow[3], #labelName
                variantRow[4], #owner
                variantRow[5], #provider
                variantRow[6], #dataType
                variantRow[7], #variantName
                {"entity": variantRow[8],
                "value": variantRow[9],
                "timestamp": variantRow[10]},
                variantRow[11], #status
                training_set_variant(sqlObject.getVariantResource( "training_set_variant", "variantName", variantRow[7]))[0] #training sets
            )
        allVariantList.append(variantRow["name"])
        variantDict[variantRow["name"]] = labelVariant
    return variantDict, allVariantList

def labels(rowData):
    variantData = label_variant(sqlObject.getVariantResource("labels", "labelName", rowData[2]))
    return LabelResource(
                
                rowData[0], #type
                rowData[1], #defaultvariant
                rowData[2], #name
                variantData[0], #variantDict
                variantData[1] #allVariants
            ) 

def entities(rowData):
    return EntityResource(
                
                rowData[0], #name
                rowData[1], #type
                rowData[2], #description
                rowData[3], #status
                feature_variant(sqlObject.getVariantResource( "features_variant", "entity", rowData[0]))[1], #features
                label_variant(sqlObject.getVariantResource( "labels_variant", "entity", rowData[0]))[0], #labels
                training_set_variant(sqlObject.getVariantResource( "training_set_variant", "entity", rowData[0]))[0] #training sets
            )

def models(rowData):
    return ModelResource(
                
                rowData[0], #name
                rowData[1], #type
                rowData[2], #description
                rowData[3], #status
                feature_variant(sqlObject.getVariantResource( "features_variant", "featureName", rowData[0]))[1], #features
                label_variant(sqlObject.getVariantResource( "labels_variant", "variantName ", rowData[0]))[0], #labels
                training_set_variant(sqlObject.getVariantResource( "training_set_variant", "trainingSetName", rowData[0]))[0] #training sets
            )

def users(rowData):
    return UserResource(
                
                rowData[0], #name
                rowData[1], #type
                rowData[2],  #status
                feature_variant(sqlObject.getVariantResource( "features_variant", "owner", rowData[0]))[1], #features
                label_variant(sqlObject.getVariantResource( "labels_variant", "owner", rowData[0]))[0], #labels
                training_set_variant(sqlObject.getVariantResource( "training_set_variant", "owner", rowData[0]))[0], #training sets
                source_variant(sqlObject.getVariantResource( "sources_variant", "owner", rowData[0]))[0], #training sets
            )

def providers(rowData):
    return ProviderResource(
                
                rowData[0], #name
                rowData[1], #type
                rowData[2], #description
                rowData[3], #provider type
                rowData[4], #software
                rowData[5], #team
                rowData[6], #sources
                rowData[7], #status
                rowData[8],#serialis...
                feature_variant(sqlObject.getVariantResource( "features_variant", "provider", rowData[0]))[1], #features
                label_variant(sqlObject.getVariantResource( "labels_variant", "provider", rowData[0]))[0], #labels
                training_set_variant(sqlObject.getVariantResource( "training_set_variant", "provider", rowData[0]))[0], #training sets
            )

@app.route("/data/<type>", methods = ['POST', 'GET'])
def GetMetadataList(type):
    tableDataCursor = sqlObject.getTypeTable(type)
    allData = []
    for row in tableDataCursor:
        print(row)
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
            allData.append(entities(row))
        elif type == "users":
            allData.append(users(row))
        else:
            allData.append("INCORRECT TYPE")
    print(allData)
    return "Hello" #returns all rows in a single list

@app.route("/data/<type>/<resource>", methods = ['POST', 'GET'])
def GetMetadata(type, resource):
    feature_type = type
    resource_type = resource
    tableData = sqlObject.getTypeForResource(feature_type, resource_type)
    switcher = {
        "features": features(tableData),
        "training_sets": training_sets(tableData),
        "sources": sources(tableData),
        "labels": labels(tableData),
        "entities": entities(tableData),
        "models": models(tableData),
        "users": users(tableData),
        "providers": providers(tableData)
    }
    return switcher.get(feature_type, "Incorrect type") #returns an object of the row

if __name__ == '__main__':
    app.run(port=5000, debug=True)
