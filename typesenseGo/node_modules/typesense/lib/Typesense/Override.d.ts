import ApiCall from './ApiCall';
import { OverrideCreateSchema } from './Overrides';
export interface OverrideSchema extends OverrideCreateSchema {
    id: string;
}
export interface OverrideDeleteSchema {
    id: string;
}
export default class Override {
    private collectionName;
    private overrideId;
    private apiCall;
    constructor(collectionName: string, overrideId: string, apiCall: ApiCall);
    retrieve(): Promise<OverrideSchema>;
    delete(): Promise<OverrideDeleteSchema>;
    private endpointPath;
}
