import ApiCall from './ApiCall';
export interface CollectionAliasCreateSchema {
    collection_name: string;
}
export interface CollectionAliasSchema extends CollectionAliasCreateSchema {
    name: string;
}
export interface CollectionAliasesResponseSchema {
    aliases: CollectionAliasSchema[];
}
export default class Aliases {
    private apiCall;
    constructor(apiCall: ApiCall);
    upsert(name: string, mapping: CollectionAliasCreateSchema): Promise<CollectionAliasSchema>;
    retrieve(): Promise<CollectionAliasesResponseSchema>;
    private endpointPath;
    static get RESOURCEPATH(): string;
}
