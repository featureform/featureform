//muh types
// const (
// 	FEATURE                ResourceType = "Feature"
// 	FEATURE_VARIANT                     = "Feature variant"
// 	LABEL                               = "Label"
// 	LABEL_VARIANT                       = "Label variant"
// 	USER                                = "User"
// 	ENTITY                              = "Entity"
// 	TRANSFORMATION                      = "Transformation"
// 	TRANSFORMATION_VARIANT              = "Transformation variant"
// 	PROVIDER                            = "Provider"
// 	SOURCE                              = "Source"
// 	SOURCE_VARIANT                      = "Source variant"
// 	TRAINING_SET                        = "Training Set"
// 	TRAINING_SET_VARIANT                = "Training Set variant"
// 	MODEL                               = "Model"
// )

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

class TypeSenseClient {
  constructor(port, host, apikey) {
    this._port = port;
    this._host = host;
    this._apikey = apikey;
  }
  search(searchParameters, collection_name) {
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
      .collections(collection_name)
      .documents()
      .search(searchParameters);
    return response.then(function (jsonResp) {
      return new TypeSenseResults(jsonResp);
    });
  }
  searchStub(jsonSample) {
    return new TypeSenseResults(jsonSample);
  }
}

stubClient = new TypeSenseClient("", "", "");

sampleResponse = {
  facet_counts: [],
  found: 4,
  hits: [
    {
      document: {
        Name: "Hero",
        Type: "Entity",
        Variant: "second",
        id: "4",
      },
      highlights: [
        {
          field: "Name",
          matched_tokens: ["Hero"],
          snippet: "\u003cmark\u003eHero\u003c/mark\u003e",
        },
      ],
      text_match: 33514497,
    },
    {
      document: {
        Name: "Villain",
        Type: "Entity",
        Variant: "default",
        id: "5",
      },
      highlights: [
        {
          field: "Name",
          matched_tokens: ["Villain"],
          snippet: "\u003cmark\u003eVillain\u003c/mark\u003e",
        },
      ],
      text_match: 33514497,
    },
    {
      document: {
        Name: "Hero",
        Type: "Feature",
        Variant: "default-1",
        id: "3",
      },
      highlights: [
        {
          field: "Name",
          matched_tokens: ["hero"],
          snippet: "\u003cmark\u003ehero\u003c/mark\u003e",
        },
      ],
      text_match: 33514497,
    },
    {
      document: {
        Name: "Heroes",
        Type: "TrainingSet",
        Variant: "default",
        id: "1",
      },
      highlights: [
        {
          field: "Name",
          matched_tokens: ["Heroes"],
          snippet: "\u003cmark\u003eheroic\u003c/mark\u003e",
        },
      ],
      text_match: 33448960,
    },
  ],
  out_of: 6,
  page: 1,
  request_params: {
    collection_name: "resource",
    per_page: 10,
    q: "hero",
  },
  search_cutoff: false,
  search_time_ms: 1,
};

results = stubClient.searchStub(sampleResponse);

console.log(results.length());
console.log(results.results());
console.log(results.resultsByType());
console.log(results.resultsForType("Feature"));
console.log(results.resultsForType("Entity"));
console.log(results.resultsForType("TrainingSet"));
