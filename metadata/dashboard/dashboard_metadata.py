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
# USE THIS FUNCTION AS A MODEL FOR OTHER FUNCTIONS
def features(featureRow):
    #Store each row's variant data in a map
    variantsDict = dict()
    variantData = sqlObject.getResource("features", featureRow[0])
    allVariantList = []
    for variantRow in variantData:
        featureVariant = FeatureVariantResource(
                variantRow[0], 
                variantRow[1], 
                variantRow[2], 
                variantRow[3], 
                variantRow[4], 
                variantRow[5], 
                variantRow[6], 
                variantRow[7], 
                variantRow[8], 
                {"entity": variantRow[9],
                "value": variantRow[11],
                "timestamp": variantRow[10]},
                variantRow[12], 
                variantRow["trainingSets"]
            )
        allVariantList.append(variantRow[3])
        variantsDict[variantRow["TODO: SHOULD BE THE VARIANT VALUE, NOT NAME"]] = featureVariant
        
    #Return an object of the row
    return FeatureResource(
                featureRow[0],
                featureRow[1],
                featureRow[2],
                variantsDict,
                allVariantList
            )
# DONE
def trainingSets(rowData):
    variantDict = dict()
    variantData = sqlObject.getResource("training-sets", rowData[2])
    allVariantList = []
    for variantRow in variantData:
        trainingSetVariant = TrainingSetVariantResource(
                #created
                variantRow[0], 
                #descrition
                variantRow[1], 
                #name
                variantRow[2], 
                #owner
                variantRow[3], 
                #provider
                variantRow[4], 
                #variant
                variantRow[5], 
                #label
                variantRow[6], 
                #features
                variantRow[7], 
                #status
                variantRow[8]
            )
        allVariantList.append(variantRow[2])
        variantDict[variantRow[5]] = trainingSetVariant
    return TrainingSetResource( 
                #type
                rowData[0], 
                #defaultvariant
                rowData[1], 
                #name
                rowData[2], 
                variantDict,
                allVariantList
            )
# DONE except feature/labels and stuff
def sources(rowData):
    variantDict = dict()
    variantData = sqlObject.getResource("sources", rowData[2])
    allVariantList = []
    for variantRow in variantData:
        sourceVariant = SourceVariantResource(
                #created
                variantRow[0], 
                #description
                variantRow[1], 
                #name
                variantRow[2], 
                #sourcetype
                variantRow[3], 
                #owner
                variantRow[4], 
                #provider
                variantRow[5], 
                #variant
                variantRow[6], 
                #status
                variantRow[7], 
                #definition
                variantRow[8], 
                #labels
                variantRow["labels"], 
                #features
                variantRow["features"], 
                #training sets
                variantRow["trainingSets"] 
            )
        allVariantList.append(variantRow[2])
        variantDict[variantRow[6]] = sourceVariant
    return SourceResource( 
                #type
                rowData[0], 
                #defaultvariant
                rowData[1], 
                #name
                rowData[2], 
                variantDict,
                allVariantList
            )

# make the variables like "features()". remove json
def labels(rowData):
    variantDict = dict()
    variantData = sqlObject.getResource("labels", rowData["name"])
    allVariantList = []
    for variantRow in variantData:
        labelVariant = LabelVariantResource(
                variantRow[0], 
                variantRow[1], 
                variantRow[2], 
                variantRow[3], 
                variantRow[4], 
                variantRow[5], 
                variantRow[6], 
                variantRow[7], 
                variantRow[8], 
                variantRow[9], 
                variantRow[10], 
                {"entity": variantRow["source_entity"],
                "value": variantRow["source_value"],
                "timestamp": variantRow["source_timestamp"]},
                variantRow["source"],
                variantRow["trainingSets"],
                variantRow["location"],

            )
        allVariantList.append(variantRow["name"])
        variantDict[variantRow["name"]] = labelVariant
    return LabelResource(
                #type
                rowData[0], 
                #defaultvariant
                rowData[1], 
                #name
                rowData[2], 
                variantDict,
                allVariantList
            ) 

def entities(rowData):
    return EntityResource(
                #name
                rowData[0], 
                #type
                rowData[1],
                #description
                rowData[2],
                #status
                rowData[3],
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"]
            )

def models(rowData):
    return ModelResource(
                #name
                rowData[0], 
                #type
                rowData[1],
                #description
                rowData[2],
                #status
                rowData[3],
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"], 
            )

def users(rowData):
    return UserResource(
                #name
                rowData[0], 
                #type
                rowData[1],
                #status
                rowData[2],
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"], 
                rowData["sources"]
            )

def providers(rowData):
    return ProviderResource(
                #name
                rowData[0], 
                #type
                rowData[1], 
                #description
                rowData[2], 
                #provider type
                rowData[3], 
                #software
                rowData[4], 
                #team
                rowData[5], 
                #sources
                rowData[6],
                #status
                rowData[7], 
                #serialis...
                rowData[8],
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"]
            )

# I HAVE REMOVED ALL REFERENCES TO JSON SINCE WE'RE NOT USING JSON ANYWHERE ANYMORE
@app.route("/data/:type")
def GetMetadataList():
    feature_type = {type}
    tableDataCursor = sqlObject.getTypeTable(feature_type)
    allData = []
    for row in tableDataCursor:
        switcher = {
            "features": features(row),
            "training-sets": trainingSets(row),
            "sources": sources(row),
            "labels": labels(row),
            "entities": entities(row),
            "models": models(row),
            "users": users(row),
            "providers": providers(row)
        }
        allData.append(switcher.get(feature_type, "Incorrect type")) #returns an object of the row and appends it to the list
    
    return allData #returns all rows in a single list

@app.route("/data/:type/:resource")
def GetMetadata():
    feature_type = {type}
    resource_type = {resource}
    tableData = sqlObject.getTypeForResource(feature_type, resource_type)
    switcher = {
        "features": features(tableData),
        "training-sets": trainingSets(tableData),
        "sources": sources(tableData),
        "labels": labels(tableData),
        "entities": entities(tableData),
        "models": models(tableData),
        "users": users(tableData),
        "providers": providers(tableData)
    }
    return switcher.get(feature_type, "Incorrect type") #returns an object of the row