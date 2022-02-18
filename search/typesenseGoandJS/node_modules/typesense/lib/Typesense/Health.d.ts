import ApiCall from './ApiCall';
export interface HealthResponse {
    status: string;
}
export default class Health {
    private apiCall;
    constructor(apiCall: ApiCall);
    retrieve(): Promise<HealthResponse>;
}
