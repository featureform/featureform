import resource
from flask import Flask
import json

app = Flask(__name__)

@app.route("/data/:type")
def GetMetadataList():
    feature_type = {type}
    file_path = "../../dashboard/data/lists/wine-data.json"
    f = open(file_path)
    data = json.load(f)
    json_type = data[feature_type]
    return json.dumps(json_type, indent = 4)

@app.route("/data/:type/:resource")
def GetMetadata():
    feature_type = {type}
    resource_type = {resource}
    file_path = "../../dashboard/data/"+feature_type+"/"+resource_type+".json"
    f = open(file_path)
    data = json.load(f)
    return json.dumps(data, indent = 4)
