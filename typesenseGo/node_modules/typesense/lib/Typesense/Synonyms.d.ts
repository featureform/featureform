import ApiCall from './ApiCall';
import { SynonymSchema } from './Synonym';
export interface SynonymCreateSchema {
    synonyms: string[];
    root?: string;
}
export interface SynonymsRetrieveSchema {
    synonyms: SynonymSchema[];
}
export default class Synonyms {
    private collectionName;
    private apiCall;
    constructor(collectionName: string, apiCall: ApiCall);
    upsert(synonymId: string, params: SynonymCreateSchema): Promise<SynonymSchema>;
    retrieve(): Promise<SynonymsRetrieveSchema>;
    private endpointPath;
    static get RESOURCEPATH(): string;
}
