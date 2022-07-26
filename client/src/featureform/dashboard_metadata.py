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

def variant_organiser(variant_list):
    variant_dict = dict()

    for variant in variant_list:
        name = variant["name"]
        if name in variant_dict: #Check if the key exists
            variant_dict[name].append(variant)
        else:
            variant_dict[name] = [variant]
    
    return variant_dict

def feature_variant(variant_data):
    variant_dict = dict()
    variant_list = []
    variants = []
    
    for variant_row in variant_data:
        feature_variant = FeatureVariantResource(
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

        variant_list.append(variant_row['variantName'])
        variant_dict[variant_row['variantName']] = feature_variant
        variants.append(feature_variant)
    return variant_dict, variant_list, variants

def features(feature_row):
    variant_data = feature_variant(sqlObject.getVariantResource("feature_variant", "featureName",feature_row['name']))
    #Return an object of the row
    return FeatureResource(
                feature_row['name'], #name
                feature_row['defaultVariant'], #defaultVariant
                feature_row['type'], #type
                variant_data[0], #variantsDict
                variant_data[1] #All Variants
            ).toDictionary()

def training_set_variant(variant_data):
    variant_dict = dict()
    variant_list = []
    variants = []

    for variant_row in variant_data:
        trainingset_variant = TrainingSetVariantResource(
                
                variant_row['created'], #created
                variant_row['description'], #description  
                variant_row['trainingSetName'], #trainingSetName
                variant_row['owner'], #owner
                variant_row['variantName'], #variantName
                {"Name":variant_row['labelName'],
                "Variant":variant_row['labelVariant']}, #label
                variant_row['status'], #status
                variant_organiser(feature_variant(get_training_set_features(variant_row['features']))[2]),
            ).toDictionary()
        variant_list.append(variant_row['variantName'])
        variant_dict[variant_row['variantName']] = trainingset_variant
        variants.append(trainingset_variant)

    return variant_dict, variant_list, variants
    
def get_training_set_features(feature_list):
    # parse the feature_list str(list of tuples)
    feature_variant_tuple = []
    features = [tuple(feature_tuple.split('\', \'')) for feature_tuple in re.findall("\(\'(.*?)\'\)", feature_list)]
    # Iterate throigh the list and get one large tuple of tuples
    for feature in features:
        feature_variant_tuple += sqlObject.getNameVariant("feature_variant", "featureName", feature[0], "variantName", feature[1])
    
    return feature_variant_tuple

def training_sets(row_data):
    variant_data = training_set_variant(sqlObject.getVariantResource("training_set_variant", "trainingSetName", row_data['name']))
    return TrainingSetResource( 
                row_data['type'], #type
                row_data['defaultVariant'], #defaultvariant
                row_data['name'], #name
                variant_data[0], #variantsDict
                variant_data[1] #all variants
            ).toDictionary()

def source_variant(variant_data):
    variant_dict = dict()
    variant_list = []
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
                variant_organiser(label_variant(sqlObject.getNameVariant( "labels_variant", "sourceName", variant_row['name'], "sourceVariant", variant_row['variant']))[2]), #labels
                variant_organiser(feature_variant(sqlObject.getNameVariant( "feature_variant", "sourceName", variant_row['name'],"sourceVariant",variant_row['variant']))[2]), #features
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "variantName", variant_row['variant']))[2]) #training sets
            ).toDictionary()
        variant_list.append(variant_row['name'])
        variant_dict[variant_row['variant']] = source_variant
        variants.append(source_variant)
    
    return variant_dict, variant_list, variants

def sources(row_data):
    variant_data = source_variant(sqlObject.getVariantResource("source_variant", "sourceName", row_data['name']))
    return SourceResource( 
                row_data['type'], #type
                row_data['defaultVariant'], #defaultVariant
                #source_variant(sqlObject.getNameVariant("source_variant", "sourceName", row_data[2], "variantName", row_data[1]))[0], #defaultvariant
                row_data['name'], #name
                variant_data[0], #variants
                variant_data[1] #all variants
            ).toDictionary()
    
def label_variant(variant_data):
    variant_dict = dict()
    variant_list = []
    variants = []

    for variant_row in variant_data:
        label_tuple = str((variant_row['labelName'], variant_row['variantName']))
        label_variant = LabelVariantResource(
                variant_row['created'], #created
                variant_row['description'], #description
                variant_row['entity'], #entity
                variant_row['labelName'], #labelName
                variant_row['owner'], #owner
                variant_row['provider'], #provider
                variant_row['dataType'], #dataType
                variant_row['variantName'], #variantName
                {"entity": variant_row['sourceEntity'],
                "value": variant_row['sourceValue'],
                "timestamp": variant_row['sourceTimestamp']},
                variant_row['status'], #status
                {"Name":variant_row['sourceName'],
                "Variant":variant_row['sourceVariant']}, #source
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "label", label_tuple))[2]) #training sets
            ).toDictionary()
        
        variant_list.append(variant_row['variantName'])
        variant_dict[variant_row['variantName']] = label_variant
        variants.append(label_variant)
    return variant_dict, variant_list, variants

def labels(row_data):
    variant_data = label_variant(sqlObject.getVariantResource("labels_variant", "labelName", row_data['name']))
    return LabelResource(
                
                row_data['type'], #type
                row_data['defaultVariant'], #defaultvariant
                row_data['name'], #name
                variant_data[0], #variant_dict
                variant_data[1] #allVariants
            ).toDictionary()

def entities(row_data):
    return EntityResource(
                
                row_data['name'], #name
                row_data['type'], #type
                row_data['description'], #description
                row_data['status'], #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "entity", row_data['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "entity", row_data['name']))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "label", row_data['name']))[2]) #training sets
            ).toDictionary()

def models(row_data):
    return ModelResource(
                row_data['name'], #name
                row_data['type'], #type
                row_data['description'], #description
                row_data['status'], #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "featureName", row_data['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "variantName ", row_data['name']))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "trainingSetName", row_data['name']))[2]) #training sets
            ).toDictionary()

def users(row_data):
    return UserResource(
                row_data['name'], #name
                row_data['type'], #type
                row_data['status'],  #status
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "owner", row_data['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "owner", row_data['name']))[2]), #labels
                variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "owner", row_data['name']))[2]), #training sets
                variant_organiser(source_variant(sqlObject.getVariantResource( "source_variant", "owner", row_data['name']))[2]), #training sets
            ).toDictionary()

def providers(row_data):
    return ProviderResource(
                
                row_data['name'], #name
                row_data['type'], #type
                row_data['description'], #description
                row_data['providerType'], #provider type
                row_data['software'], #software
                row_data['team'], #team
                variant_organiser(source_variant(sqlObject.getVariantResource( "source_variant", "provider", row_data['name']))[2]), #sources
                row_data['status'], #status
                row_data['serializedConfig'],#serialis...
                variant_organiser(feature_variant(sqlObject.getVariantResource( "feature_variant", "provider", row_data['name']))[2]), #features
                variant_organiser(label_variant(sqlObject.getVariantResource( "labels_variant", "provider", row_data['name']))[2]), #labels
                #variant_organiser(training_set_variant(sqlObject.getVariantResource( "training_set_variant", "provider", row_data[0]))[2]), #training sets
            ).toDictionary()

@app.route("/data/<type>", methods = ['POST', 'GET'])
@cross_origin(allow_headers=['Content-Type'])
def GetMetadataList(type):
    type = type.replace("-", "_")
    table_data_cursor = sqlObject.getTypeTable(type)
    all_data = []
    for row in table_data_cursor:
        if type == "features":
            all_data.append(features(row))
        elif type == "training_sets":
            all_data.append(training_sets(row))
        elif type == "sources":
            all_data.append(sources(row))
        elif type == "labels":
            all_data.append(labels(row))
        elif type == "entities":
            all_data.append(entities(row))
        elif type == "models":
            all_data.append(models(row))
        elif type == "users":
            all_data.append(users(row))
        elif type == "providers":
            all_data.append(providers(row))
        else:
            all_data.append("INCORRECT TYPE")

    response = app.response_class(
        response=json.dumps(all_data),
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
            data_as_list =  features(row)
        elif type == "training_sets":
            data_as_list =  training_sets(row)
        elif type == "sources":
            data_as_list =  sources(row)
        elif type == "labels":
            data_as_list =  labels(row)
        elif type == "entities":
            data_as_list =  entities(row)
        elif type == "models":
            data_as_list =  models(row)
        elif type == "users":
            data_as_list =  users(row)
        elif type == "providers":
            data_as_list =  providers(row)
        else:
            data_as_list = "INCORRECT TYPE"

        response = app.response_class(
            response=json.dumps(data_as_list),
            status=200,
            mimetype='application/json'
        )
        return response
