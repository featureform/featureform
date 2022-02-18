const Typesense = require('typesense')
class Document {
    constructor(hit){
        this.metadata = hit["document"];
        this.type = this.metadata["type"];
    }
}
class Parseresults{
    constructor(results) {
        this.allresults= results;
        this.numberofhitsfound= (this.allresults)["found"];
        this.hits=(this.allresults)["hits"];
        let listofhits = {}
        for (let i =0; i<(this.hits).length;i++){
            let hit = new Document(this.hits[i])
            listofhits[hit.type]= hit.metadata
        }
        this.hitsbytype=listofhits
    }
}
class StartSearch {
    constructor(port, host, apikey, type="typesense"){ //assuming this is typesense
        this.port=port;
        this.host=host;
        this.type=type;
        this.apikey=apikey;
    }
    search(searchParameters, collection_name){
        let client = new Typesense.Client({
            'nodes': [{
              'host': this.host, 
              'port': this.port,
              'protocol': 'http'
            }],
            'apiKey': this.apikey,
            'connectionTimeoutSeconds': 2
          })
        this.results = client.collections(collection_name)
        .documents()
        .search(searchParameters)
    }
}
//const val = '{"facet_counts":[],"found":3,"hits":[{"document":{"description":"Vector generated based on user preferences, normalized","id":"10","name":"User sample preferences","type":"Space","variant":"normalized variant"},"highlights":[{"field":"name","matched_tokens":["sample"],"snippet":"User \u003cmark\u003esample\u003c/mark\u003e preferences"}],"text_match":33317888},{"document":{"description":"Wine data collected from a sample of Napa Valley wines","id":"9","name":"Wine Quality Sample File","type":"Data Source","variant":"default-variant"},"highlights":[{"field":"name","matched_tokens":["Sample"],"snippet":"Wine Quality \u003cmark\u003eSample\u003c/mark\u003e File"}],"text_match":33317888},{"document":{"description":"Online provider","id":"3","name":"Sample online provider","type":"Provider","variant":"default-variant"},"highlights":[{"field":"name","matched_tokens":["Sample"],"snippet":"\u003cmark\u003eSample\u003c/mark\u003e online provider"}],"text_match":33317888}],"out_of":11,"page":1,"request_params":{"collection_name":"featuredata","per_page":10,"q":"smple"},"search_cutoff":false,"search_time_ms":17}';
let search1 = new StartSearch("8108", "localhost", "xyz")
let searchParameters = {
    'q'         : 'smple',
    'query_by'  : 'name'
  }
search1.search(searchParameters, "featuredata")
search1.results.then(response => {
    console.log(response)
    let myresult= new Parseresults(response);
    console.log(myresult.hitsbytype)
}).catch(error => {
console.log(error)
});