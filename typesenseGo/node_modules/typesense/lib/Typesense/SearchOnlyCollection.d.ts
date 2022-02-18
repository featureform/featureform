import { DocumentSchema, SearchableDocuments } from './Documents';
import ApiCall from './ApiCall';
export declare class SearchOnlyCollection<T extends DocumentSchema = {}> {
    private readonly name;
    private readonly apiCall;
    private readonly configuration;
    private readonly _documents;
    constructor(name: string, apiCall: ApiCall, configuration: any);
    documents(): SearchableDocuments<T>;
}
