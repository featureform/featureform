const Typesense = require("typesense");

class TypeSenseResults {
  constructor(typesenseresponse) {
    this._rawresults = typesenseresponse;
    this._length = this._rawresults["found"];
    let hits = this._rawresults["hits"];
    let listofdocuments = [];
    for (let i = 0; i < hits.length; i++) {
      let doc = hits[i]["document"];
      listofdocuments.push(doc);
    }
    this._listofresults = listofdocuments;
    let results = this._listofresults;
    let dictionary = {};
    for (let i = 0; i < results.length; i++) {
      let type = results[i]["Type"];
      if (dictionary[type]) {
        dictionary[type].push(results[i]);
      } else {
        dictionary[type] = [results[i]];
      }
    }
    this._resultsByType = dictionary;
  }
  length() {
    return this._length;
  }
  results() {
    return this._listofresults;
  }
  resultsByType() {
    return this._resultsByType;
  }
  resultsForType(type) {
    let dictionary = this._resultsByType;
    return dictionary[type];
  }
}

export default class TypesenseClient {
  constructor(port, host, apikey) {
    this._port = port;
    this._host = host;
    this._apikey = apikey;
  }
  search(query) {
    let searchParams = { q: query, query_by: "Name" };
    let client = new Typesense.Client({
      nodes: [
        {
          host: this._host,
          port: this._port,
          protocol: "http",
        },
      ],
      apiKey: this._apikey,
      connectionTimeoutSeconds: 2,
    });
    let response = client
      .collections("resource")
      .documents()
      .search(searchParams);
    return response.then(function (jsonResp) {
      return new TypeSenseResults(jsonResp);
    });
  }
}
