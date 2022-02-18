import ApiCall from './ApiCall';
import Configuration from './Configuration';
import { DocumentSchema, SearchParams, SearchResponse } from './Documents';
export interface MultiSearchRequestSchema<T> extends SearchParams<T> {
    collection?: string;
}
export interface MultiSearchRequestsSchema<T> {
    searches: MultiSearchRequestSchema<T>[];
}
export interface MultiSearchResponse<T> {
    results: SearchResponse<T>[];
}
export default class MultiSearch<T extends DocumentSchema = {}> {
    private apiCall;
    private configuration;
    private useTextContentType;
    private requestWithCache;
    constructor(apiCall: ApiCall, configuration: Configuration, useTextContentType?: boolean);
    perform(searchRequests: MultiSearchRequestsSchema<T>, commonParams?: Partial<MultiSearchRequestSchema<T>>, { cacheSearchResultsForSeconds }?: {
        cacheSearchResultsForSeconds?: number;
    }): Promise<MultiSearchResponse<T>>;
}
