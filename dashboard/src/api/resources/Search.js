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
    return this
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
  constructor(url) {
    this._url = url
  }
  search(query) {
    var queryString = this._url + query
    return fetch(queryString, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then(function (res) {
          return res.json().then((json_data) => {
            return new TypeSenseResults(json_data)} );
      })
      .catch((error) => {
        console.error(error);
      });
  }
}
