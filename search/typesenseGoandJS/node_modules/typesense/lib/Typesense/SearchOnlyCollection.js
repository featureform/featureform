"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SearchOnlyCollection = void 0;
const SearchOnlyDocuments_1 = require("./SearchOnlyDocuments");
class SearchOnlyCollection {
    constructor(name, apiCall, configuration) {
        this.name = name;
        this.apiCall = apiCall;
        this.configuration = configuration;
        this._documents = new SearchOnlyDocuments_1.SearchOnlyDocuments(this.name, this.apiCall, this.configuration);
    }
    documents() {
        return this._documents;
    }
}
exports.SearchOnlyCollection = SearchOnlyCollection;
//# sourceMappingURL=SearchOnlyCollection.js.map