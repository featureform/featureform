import { ConfigurationOptions } from './Configuration';
import MultiSearch from './MultiSearch';
import { DocumentSchema } from './Documents';
import { SearchOnlyCollection } from './SearchOnlyCollection';
export default class SearchClient {
    readonly multiSearch: MultiSearch;
    private readonly configuration;
    private readonly apiCall;
    private readonly individualCollections;
    constructor(options: ConfigurationOptions);
    collections<TDocumentSchema extends DocumentSchema = {}>(collectionName: string): SearchOnlyCollection<TDocumentSchema> | SearchOnlyCollection;
}
