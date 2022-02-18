const Typesense = require('typesense')

class TypeSenseResults{
    constructor(results) {
        this._rawresults= results;
    }
    length(){
        this._length= this._rawresults["found"]
        return this._length
    }
    results(){
        let hits=(this._rawresults)["hits"];
        let listofdocuments = []
        for (let i =0; i<(hits).length;i++){
            let doc = hits[i]["document"]
            listofdocuments.push(doc)
        }
        this._listofresults=listofdocuments
        return this._listofresults
    }
    resultsByType(){
        let results = this.results()
        let dictionary={}
        for (let i=0; i<results.length;i++){
            let type = results[i]["type"]
            dictionary[type]=results[i]
        }
        this._resultsByType=dictionary
        return this._resultsByType
    }
    resultsForType(type){
        let dictionary = this.resultsByType()
        return dictionary[type]
    }
}

class TypesenseClient {
    constructor(port, host, apikey){
        this._port=port;
        this._host=host;
        this._apikey=apikey;
    }
    search(searchParameters, collection_name){
        let client = new Typesense.Client({
            'nodes': [{
              'host': this._host, 
              'port': this._port,
              'protocol': 'http'
            }],
            'apiKey': this._apikey,
            'connectionTimeoutSeconds': 2
          })
        let response = client.collections(collection_name)
        .documents()
        .search(searchParameters)
        return response.then(function(jsonResp) {
            return new TypeSenseResults(jsonResp)
        })
    }
}