import ApiCall from './ApiCall';
import { CollectionFieldSchema, CollectionSchema } from './Collection';
export interface CollectionCreateSchema {
    name: string;
    default_sorting_field?: string;
    fields: CollectionFieldSchema[];
    symbols_to_index?: string[];
    token_separators?: string[];
}
export default class Collections {
    private apiCall;
    constructor(apiCall: ApiCall);
    create(schema: CollectionCreateSchema): Promise<CollectionSchema>;
    retrieve(): Promise<CollectionSchema[]>;
    static get RESOURCEPATH(): string;
}
