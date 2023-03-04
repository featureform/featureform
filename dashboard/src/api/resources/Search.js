const { MeiliSearch } = require('meilisearch')

class MeiliSearchResults {
    constructor(meilisearchresponse) {
        this._rawresults = meilisearchresponse;
        this._length = this._rawresults["estimatedTotalHits"];
        this._hits = this._rawresults["hits"];
        let dictionary = {};
        for (let i = 0; i < this._hits.length; i++) {
            let type = this._hits[i]["Type"];
            if (dictionary[type]) {
                dictionary[type].push(this._hits[i]);
            } else {
                dictionary[type] = [this._hits[i]];
            }
        }
        this._resultsByType = dictionary;
    }
    length() {
        return this._length;
    }
    results() {
        return this._hits;
    }
    resultsByType() {
        return this._resultsByType;
    }
    resultsForType(type) {
        let dictionary = this._resultsByType;
        return dictionary[type];
    }
}

module.exports = class MeiliSearchClient {
    constructor(port, host, apikey) {
        this._port = port;
        this._host = host;
        this._apikey = apikey;
    }
    search(query) {
        let searchParams = { q: query, query_by: "Name" };
        let client = new MeiliSearch({
            host: "http://"+this._host+":"+this._port,
        });
        let response = client.index("resources").search(query);
        return response.then(function (jsonResp) {
            return new MeiliSearchResults(jsonResp);
        });
    }
};