"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const RequestWithCache_1 = __importDefault(require("./RequestWithCache"));
const RESOURCEPATH = '/multi_search';
class MultiSearch {
    constructor(apiCall, configuration, useTextContentType = false) {
        this.apiCall = apiCall;
        this.configuration = configuration;
        this.useTextContentType = useTextContentType;
        this.requestWithCache = new RequestWithCache_1.default();
    }
    perform(searchRequests, commonParams = {}, { cacheSearchResultsForSeconds = this.configuration.cacheSearchResultsForSeconds } = {}) {
        let additionalHeaders = {};
        if (this.useTextContentType) {
            additionalHeaders['content-type'] = 'text/plain';
        }
        let additionalQueryParams = {};
        if (this.configuration.useServerSideSearchCache === true) {
            additionalQueryParams['usecache'] = true;
        }
        const queryParams = Object.assign({}, commonParams, additionalQueryParams);
        return this.requestWithCache.perform(this.apiCall, this.apiCall.post, [RESOURCEPATH, searchRequests, queryParams, additionalHeaders], { cacheResponseForSeconds: cacheSearchResultsForSeconds });
    }
}
exports.default = MultiSearch;
//# sourceMappingURL=MultiSearch.js.map