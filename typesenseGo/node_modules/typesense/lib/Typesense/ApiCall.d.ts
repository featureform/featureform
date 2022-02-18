import { AxiosRequestConfig, AxiosResponse, Method } from 'axios';
import TypesenseError from './Errors/TypesenseError';
import Configuration, { NodeConfiguration } from './Configuration';
interface Node extends NodeConfiguration {
    isHealthy: boolean;
    index: string | number;
}
export default class ApiCall {
    private configuration;
    private readonly apiKey;
    private readonly nodes;
    private readonly nearestNode;
    private readonly connectionTimeoutSeconds;
    private readonly healthcheckIntervalSeconds;
    private readonly retryIntervalSeconds;
    private readonly sendApiKeyAsQueryParam;
    private readonly numRetriesPerRequest;
    private readonly additionalUserHeaders;
    private readonly logger;
    private currentNodeIndex;
    constructor(configuration: Configuration);
    get<T extends any>(endpoint: string, queryParameters?: any, { abortSignal, responseType }?: {
        abortSignal?: any;
        responseType?: AxiosRequestConfig['responseType'];
    }): Promise<T>;
    delete<T extends any>(endpoint: string, queryParameters?: any): Promise<T>;
    post<T extends any>(endpoint: string, bodyParameters?: any, queryParameters?: any, additionalHeaders?: any): Promise<T>;
    put<T extends any>(endpoint: string, bodyParameters?: any, queryParameters?: any): Promise<T>;
    patch<T extends any>(endpoint: string, bodyParameters?: any, queryParameters?: any): Promise<T>;
    performRequest<T extends any>(requestType: Method, endpoint: string, { queryParameters, bodyParameters, additionalHeaders, abortSignal, responseType }: {
        queryParameters?: any;
        bodyParameters?: any;
        additionalHeaders?: any;
        abortSignal?: any;
        responseType?: AxiosRequestConfig['responseType'];
    }): Promise<T>;
    getNextNode(requestNumber?: number): Node;
    nodeDueForHealthcheck(node: any, requestNumber?: number): boolean;
    initializeMetadataForNodes(): void;
    setNodeHealthcheck(node: any, isHealthy: any): void;
    uriFor(endpoint: string, node: any): string;
    defaultHeaders(): any;
    timer(seconds: any): Promise<void>;
    customErrorForResponse(response: AxiosResponse, messageFromServer: string): TypesenseError;
}
export {};
