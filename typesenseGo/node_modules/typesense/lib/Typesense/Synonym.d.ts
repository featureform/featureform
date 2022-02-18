import ApiCall from './ApiCall';
import { SynonymCreateSchema } from './Synonyms';
export interface SynonymSchema extends SynonymCreateSchema {
    id: string;
}
export interface SynonymDeleteSchema {
    id: string;
}
export default class Synonym {
    private collectionName;
    private synonymId;
    private apiCall;
    constructor(collectionName: string, synonymId: string, apiCall: ApiCall);
    retrieve(): Promise<SynonymSchema>;
    delete(): Promise<SynonymDeleteSchema>;
    private endpointPath;
}
