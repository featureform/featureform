import ApiCall from './ApiCall';
export interface KeyCreateSchema {
    actions: string[];
    collections: string[];
    description?: string;
    value?: string;
    expires_at?: number;
}
export interface KeyDeleteSchema {
    id: number;
}
export interface KeySchema extends KeyCreateSchema {
    id: number;
}
export default class Key {
    private id;
    private apiCall;
    constructor(id: number, apiCall: ApiCall);
    retrieve(): Promise<KeySchema>;
    delete(): Promise<KeyDeleteSchema>;
    private endpointPath;
}
