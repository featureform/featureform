import RequestWithCache from './RequestWithCache';
import ApiCall from './ApiCall';
import Configuration from './Configuration';
import type { SearchableDocuments, SearchOptions, SearchParams, SearchResponse } from './Documents';
export declare class SearchOnlyDocuments<T> implements SearchableDocuments<T> {
    protected collectionName: string;
    protected apiCall: ApiCall;
    protected configuration: Configuration;
    protected requestWithCache: RequestWithCache;
    constructor(collectionName: string, apiCall: ApiCall, configuration: Configuration);
    search(searchParameters: SearchParams<T>, { cacheSearchResultsForSeconds, abortSignal }?: SearchOptions): Promise<SearchResponse<T>>;
    protected endpointPath(operation?: string): string;
    static get RESOURCEPATH(): string;
}
