/// <reference types="node" />
import type { ReadStream } from 'fs';
import ApiCall from './ApiCall';
import Configuration from './Configuration';
import { SearchOnlyDocuments } from './SearchOnlyDocuments';
export interface DeleteQuery {
    filter_by: string;
    batch_size?: number;
}
export interface DeleteResponse {
    num_deleted: number;
}
interface ImportResponseSuccess {
    success: true;
}
export interface ImportResponseFail {
    success: false;
    error: string;
    document: DocumentSchema;
    code: number;
}
export declare type ImportResponse = ImportResponseSuccess | ImportResponseFail;
export interface DocumentSchema extends Record<string, any> {
}
export interface SearchParams<T extends DocumentSchema> {
    q: string;
    query_by: string;
    query_by_weights?: string;
    prefix?: boolean;
    filter_by?: string;
    sort_by?: string;
    facet_by?: string;
    max_facet_values?: number;
    facet_query?: string;
    page?: number;
    per_page?: number;
    group_by?: string;
    group_limit?: number;
    include_fields?: string;
    exclude_fields?: string;
    highlight_fields?: string;
    highlight_full_fields?: string;
    highlight_affix_num_tokens?: number;
    highlight_start_tag?: string;
    highlight_end_tag?: string;
    snippet_threshold?: number;
    num_typos?: string;
    min_len_1typo?: number;
    min_len_2typo?: number;
    exhaustive_search?: boolean;
    drop_tokens_threshold?: number;
    typo_tokens_threshold?: number;
    pinned_hits?: string;
    hidden_hits?: string;
    limit_hits?: number;
    pre_segmented_query?: boolean;
    enable_overrides?: boolean;
    prioritize_exact_match?: boolean;
    search_cutoff_ms?: number;
    use_cache?: boolean;
}
export interface SearchResponseHit<T extends DocumentSchema> {
    highlights?: [
        {
            field: keyof T;
            snippet?: string;
            value?: string;
            snippets?: string[];
            indices?: string[];
            matched_tokens: string[];
        }
    ];
    document: T;
    text_match: number;
}
export interface SearchResponseFacetCountSchema<T extends DocumentSchema> {
    counts: [
        {
            count: number;
            highlighted: string;
            value: string;
        }
    ];
    field_name: keyof T;
    stats: {
        avg?: number;
        max?: number;
        min?: number;
        sum?: number;
    };
}
export interface SearchResponse<T extends DocumentSchema> {
    facet_counts?: SearchResponseFacetCountSchema<T>[];
    found: number;
    out_of: number;
    page: number;
    request_params: SearchParams<T>;
    search_time_ms: number;
    hits?: SearchResponseHit<T>[];
    grouped_hits?: {
        group_key: string[];
        hits: SearchResponseHit<T>[];
    }[];
}
export interface DocumentWriteParameters {
    dirty_values?: 'coerce_or_reject' | 'coerce_or_drop' | 'drop' | 'reject';
    action?: 'create' | 'update' | 'upsert';
}
export interface DocumentsExportParameters {
    filter_by?: string;
    include_fields?: string;
    exclude_fields?: string;
}
export interface SearchableDocuments<T> {
    search(searchParameters: SearchParams<T>, options: SearchOptions): Promise<SearchResponse<T>>;
}
export interface WriteableDocuments<T> {
    create(document: T, options: DocumentWriteParameters): Promise<T>;
    upsert(document: T, options: DocumentWriteParameters): Promise<T>;
    update(document: T, options: DocumentWriteParameters): Promise<T>;
    delete(idOrQuery: string | DeleteQuery): Promise<DeleteResponse> | Promise<T>;
    import(documents: T[] | string, options: DocumentWriteParameters): Promise<string | ImportResponse[]>;
    export(options: DocumentsExportParameters): Promise<string>;
}
export interface SearchOptions {
    cacheSearchResultsForSeconds?: number;
    abortSignal?: AbortSignal;
}
export default class Documents<T extends DocumentSchema = {}> extends SearchOnlyDocuments<T> implements WriteableDocuments<T> {
    constructor(collectionName: string, apiCall: ApiCall, configuration: Configuration);
    create(document: T, options?: DocumentWriteParameters): Promise<T>;
    upsert(document: T, options?: DocumentWriteParameters): Promise<T>;
    update(document: T, options?: DocumentWriteParameters): Promise<T>;
    delete(idOrQuery: DeleteQuery): Promise<DeleteResponse>;
    delete(idOrQuery: string): Promise<T>;
    createMany(documents: T[], options?: DocumentWriteParameters): Promise<ImportResponse[]>;
    /**
     * Import a set of documents in a batch.
     * @param {string|Array} documents - Can be a JSONL string of documents or an array of document objects.
     * @param options
     * @return {string|Array} Returns a JSONL string if the input was a JSONL string, otherwise it returns an array of results.
     */
    import(documents: string, options?: DocumentWriteParameters): Promise<string>;
    import(documents: T[], options?: DocumentWriteParameters): Promise<ImportResponse[]>;
    /**
     * Returns a JSONL string for all the documents in this collection
     */
    export(options?: DocumentsExportParameters): Promise<string>;
    /**
     * Returns a NodeJS readable stream of JSONL for all the documents in this collection.
     */
    exportStream(options?: DocumentsExportParameters): Promise<ReadStream>;
}
export {};
