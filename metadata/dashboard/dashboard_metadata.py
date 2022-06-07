import resource
from flask import Flask
import json

app = Flask(__name__)

def features(rowData, sqlObject):

    #Store each row's variant data in a map
    variantData = dict()
    _, variantDataJSON = sqlObject.getResource("features", rowData["name"])
    for variantRowJSON in variantDataJSON:
        variantRowDictionary = json.loads(variantRowJSON) #puts each json row into a dictionary
        featureVariant = FeatureVariantResource(
                variantRowDictionary["created"], 
                variantRowDictionary["description"], 
                variantRowDictionary["entity"], 
                variantRowDictionary["name"], 
                variantRowDictionary["owner"], 
                variantRowDictionary["provider"], 
                variantRowDictionary["dataType"], 
                variantRowDictionary["variant"], 
                variantRowDictionary["status"], 
                variantRowDictionary["location"], 
                variantRowDictionary["source"], 
                variantRowDictionary["trainingSets"] 
            )
        variantData[variantRowDictionary["name"]] = featureVariant

    #Return an object of the row
    return FeatureResource(
                rowData["allVariants"], 
                rowData["type"], 
                rowData["defaultVariant"], 
                rowData["name"], 
                variantData
            )

def trainingSets(rowData):
    variantData = dict()
    _, variantDataJSON = sqlObject.getResource("training-sets", rowData["name"])
    for variantRowJSON in variantDataJSON:
        variantRowDictionary = json.loads(variantRowJSON) #puts each json row into a dictionary
        trainingSetVariant = TrainingSetVariantResource(
                variantRowDictionary["created"], 
                variantRowDictionary["description"], 
                variantRowDictionary["name"], 
                variantRowDictionary["owner"], 
                variantRowDictionary["provider"], 
                variantRowDictionary["variant"], 
                variantRowDictionary["status"], 
                variantRowDictionary["label"], 
                variantRowDictionary["features"]
            )
        variantData[variantRowDictionary["name"]] = trainingSetVariant
    return TrainingSetResource(
                rowData["allVariants"], 
                rowData["type"], 
                rowData["defaultVariant"], 
                rowData["name"], 
                variantData
            )

def sources(rowData):
    variantData = dict()
    _, variantDataJSON = sqlObject.getResource("sources", rowData["name"])
    for variantRowJSON in variantDataJSON:
        variantRowDictionary = json.loads(variantRowJSON) #puts each json row into a dictionary
        sourceVariant = SourceVariantResource(
                variantRowDictionary["created"], 
                variantRowDictionary["description"], 
                variantRowDictionary["name"], 
                variantRowDictionary["sourceType"], 
                variantRowDictionary["owner"], 
                variantRowDictionary["provider"], 
                variantRowDictionary["variant"], 
                variantRowDictionary["status"], 
                variantRowDictionary["definition"], 
                variantRowDictionary["labels"], 
                variantRowDictionary["features"], 
                variantRowDictionary["trainingSets"] 
            )
        variantData[variantRowDictionary["name"]] = sourceVariant
    return SourceResource(
                rowData["allVariants"], 
                rowData["type"], 
                rowData["defaultVariant"], 
                rowData["name"], 
                variantData
            )

def labels(rowData):
    variantData = dict()
    _, variantDataJSON = sqlObject.getResource("labels", rowData["name"])
    for variantRowJSON in variantDataJSON:
        variantRowDictionary = json.loads(variantRowJSON) #puts each json row into a dictionary
        labelVariant = LabelVariantResource(
                variantRowDictionary["created"], 
                variantRowDictionary["description"], 
                variantRowDictionary["entity"], 
                variantRowDictionary["name"], 
                variantRowDictionary["dataType"], 
                variantRowDictionary["owner"], 
                variantRowDictionary["provider"], 
                variantRowDictionary["variant"], 
                variantRowDictionary["status"], 
                variantRowDictionary["source"], 
                variantRowDictionary["trainingSets"] 
                variantRowDictionary["location"], 
            )
        variantData[variantRowDictionary["name"]] = labelVariant
    return LabelResource(
                rowData["allVariants"], 
                rowData["type"], 
                rowData["defaultVariant"], 
                rowData["name"], 
                variantData
            ) 

def entities(rowData):
    return EntityResource(
                rowData["description"], 
                rowData["type"], 
                rowData["name"], 
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"] 
                rowData["status"]
            )

def models(rowData):
    return ModelResource(
                rowData["name"], 
                rowData["type"],
                rowData["description"],
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"], 
                rowData["status"]
            )

def users(rowData):
    return UserResource(
                rowData["name"], 
                rowData["type"],
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"], 
                rowData["sources"],
                rowData["status"]
            )

def providers(rowData):
    return ProviderResource(
                rowData["name"], 
                rowData["type"], 
                rowData["description"], 
                rowData["providerType"], 
                rowData["software"], 
                rowData["team"], 
                rowData["sources"], 
                rowData["features"], 
                rowData["labels"], 
                rowData["trainingSets"] 
                rowData["status"], 
                rowData["serializedConfig"]
            )


@app.route("/data/:type")
def GetMetadataList():
    feature_type = {type}
    sqlObject = SQLiteTest()
    tableData = sqlObject.getType(feature_type)

    allData = []
    for rowJSON in tableData:
        rowDictionary = json.loads(rowJSON) #puts each json row into a dictionary
        switcher = {
            "features": features(rowDictionary, sqlObject),
            "training-sets": trainingSets(rowDictionary, sqlObject),
            "sources": sources(rowDictionary, sqlObject),
            "labels": labels(rowDictionary, sqlObject),
            "entities": entities(rowDictionary),
            "models": models(rowDictionary),
            "users": users(rowDictionary),
            "providers" providers(rowDictionary)
        }
        allData.append(switcher.get(feature_type, "Incorrect type")) #returns an object of the row and appends it to the list

    return allData #returns all rows in a single list

@app.route("/data/:type/:resource")
def GetMetadata():
    feature_type = {type}
    resource_type = {resource}
    tableData, variantData = sqlObject.getResource(feature_type, resource_type)

    rowDictionary = json.loads(tableData) #puts each json row into a dictionary
        switcher = {
            "features": features(rowDictionary, sqlObject),
            "training-sets": trainingSets(rowDictionary, sqlObject),
            "sources": sources(rowDictionary, sqlObject),
            "labels": labels(rowDictionary, sqlObject),
            "entities": entities(rowDictionary),
            "models": models(rowDictionary),
            "users": users(rowDictionary),
            "providers" providers(rowDictionary)
        }
    return switcher.get(feature_type, "Incorrect type" #returns an object of the row
