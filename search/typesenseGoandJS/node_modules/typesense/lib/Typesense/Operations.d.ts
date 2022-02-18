import ApiCall from './ApiCall';
export default class Operations {
    private apiCall;
    constructor(apiCall: ApiCall);
    perform(operationName: 'vote' | 'snapshot' | string, queryParameters?: Record<string, any>): Promise<any>;
}
