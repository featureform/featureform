"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SearchOnlyDocuments = void 0;
const RequestWithCache_1 = __importDefault(require("./RequestWithCache"));
const Collections_1 = __importDefault(require("./Collections"));
const RESOURCEPATH = '/documents';
class SearchOnlyDocuments {
    constructor(collectionName, apiCall, configuration) {
        this.collectionName = collectionName;
        this.apiCall = apiCall;
        this.configuration = configuration;
        this.requestWithCache = new RequestWithCache_1.default();
    }
    async search(searchParameters, { cacheSearchResultsForSeconds = this.configuration.cacheSearchResultsForSeconds, abortSignal = null } = {}) {
        let additionalQueryParams = {};
        if (this.configuration.useServerSideSearchCache === true) {
            additionalQueryParams['usecache'] = true;
        }
        const queryParams = Object.assign({}, searchParameters, additionalQueryParams);
        return await this.requestWithCache.perform(this.apiCall, this.apiCall.get, [this.endpointPath('search'), queryParams, { abortSignal }], {
            cacheResponseForSeconds: cacheSearchResultsForSeconds
        });
    }
    endpointPath(operation) {
        return `${Collections_1.default.RESOURCEPATH}/${this.collectionName}${RESOURCEPATH}${operation === undefined ? '' : '/' + operation}`;
    }
    static get RESOURCEPATH() {
        return RESOURCEPATH;
    }
}
exports.SearchOnlyDocuments = SearchOnlyDocuments;
//# sourceMappingURL=SearchOnlyDocuments.js.map