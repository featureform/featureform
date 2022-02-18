import ApiCall from './ApiCall';
export interface DebugResponseSchema {
    state: number;
    version: string;
}
export default class Debug {
    private apiCall;
    constructor(apiCall: ApiCall);
    retrieve(): Promise<DebugResponseSchema>;
}
