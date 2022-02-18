"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const Errors_1 = require("./Errors");
const TypesenseError_1 = __importDefault(require("./Errors/TypesenseError"));
const APIKEYHEADERNAME = 'X-TYPESENSE-API-KEY';
const HEALTHY = true;
const UNHEALTHY = false;
class ApiCall {
    constructor(configuration) {
        this.configuration = configuration;
        this.apiKey = this.configuration.apiKey;
        this.nodes = JSON.parse(JSON.stringify(this.configuration.nodes)); // Make a copy, since we'll be adding additional metadata to the nodes
        this.nearestNode = JSON.parse(JSON.stringify(this.configuration.nearestNode));
        this.connectionTimeoutSeconds = this.configuration.connectionTimeoutSeconds;
        this.healthcheckIntervalSeconds = this.configuration.healthcheckIntervalSeconds;
        this.numRetriesPerRequest = this.configuration.numRetries;
        this.retryIntervalSeconds = this.configuration.retryIntervalSeconds;
        this.sendApiKeyAsQueryParam = this.configuration.sendApiKeyAsQueryParam;
        this.additionalUserHeaders = this.configuration.additionalHeaders;
        this.logger = this.configuration.logger;
        this.initializeMetadataForNodes();
        this.currentNodeIndex = -1;
    }
    get(endpoint, queryParameters = {}, { abortSignal = null, responseType = null } = {}) {
        return this.performRequest('get', endpoint, { queryParameters, abortSignal, responseType });
    }
    delete(endpoint, queryParameters = {}) {
        return this.performRequest('delete', endpoint, { queryParameters });
    }
    post(endpoint, bodyParameters = {}, queryParameters = {}, additionalHeaders = {}) {
        return this.performRequest('post', endpoint, { queryParameters, bodyParameters, additionalHeaders });
    }
    put(endpoint, bodyParameters = {}, queryParameters = {}) {
        return this.performRequest('put', endpoint, { queryParameters, bodyParameters });
    }
    patch(endpoint, bodyParameters = {}, queryParameters = {}) {
        return this.performRequest('patch', endpoint, { queryParameters, bodyParameters });
    }
    async performRequest(requestType, endpoint, { queryParameters = null, bodyParameters = null, additionalHeaders = {}, abortSignal = null, responseType = null }) {
        var _a, _b, _c;
        this.configuration.validate();
        const requestNumber = Date.now();
        let lastException;
        this.logger.debug(`Request #${requestNumber}: Performing ${requestType.toUpperCase()} request: ${endpoint}`);
        for (let numTries = 1; numTries <= this.numRetriesPerRequest + 1; numTries++) {
            let node = this.getNextNode(requestNumber);
            this.logger.debug(`Request #${requestNumber}: Attempting ${requestType.toUpperCase()} request Try #${numTries} to Node ${node.index}`);
            if (abortSignal && abortSignal.aborted) {
                return Promise.reject(new Error('Request aborted by caller.'));
            }
            let abortListener;
            try {
                let requestOptions = {
                    method: requestType,
                    url: this.uriFor(endpoint, node),
                    headers: Object.assign({}, this.defaultHeaders(), additionalHeaders, this.additionalUserHeaders),
                    timeout: this.connectionTimeoutSeconds * 1000,
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity,
                    responseType,
                    validateStatus: (status) => {
                        /* Override default validateStatus, which only considers 2xx a success.
                            In our case, if the server returns any HTTP code, we will handle it below.
                            We do this to be able to raise custom errors based on response code.
                         */
                        return status > 0;
                    },
                    transformResponse: [
                        (data, headers) => {
                            let transformedData = data;
                            if (headers !== undefined &&
                                typeof data === 'string' &&
                                headers['content-type'] &&
                                headers['content-type'].startsWith('application/json')) {
                                transformedData = JSON.parse(data);
                            }
                            return transformedData;
                        }
                    ]
                };
                if (queryParameters && Object.keys(queryParameters).length !== 0) {
                    requestOptions.params = queryParameters;
                }
                if (this.sendApiKeyAsQueryParam) {
                    requestOptions.params = requestOptions.params || {};
                    requestOptions.params['x-typesense-api-key'] = this.apiKey;
                }
                if (bodyParameters &&
                    ((typeof bodyParameters === 'string' && bodyParameters.length !== 0) ||
                        (typeof bodyParameters === 'object' && Object.keys(bodyParameters).length !== 0))) {
                    requestOptions.data = bodyParameters;
                }
                // Translate from user-provided AbortController to the Axios request cancel mechanism.
                if (abortSignal) {
                    const cancelToken = axios_1.default.CancelToken;
                    const source = cancelToken.source();
                    abortListener = () => source.cancel();
                    abortSignal.addEventListener('abort', abortListener);
                    requestOptions.cancelToken = source.token;
                }
                let response = await axios_1.default(requestOptions);
                if (response.status >= 1 && response.status <= 499) {
                    // Treat any status code > 0 and < 500 to be an indication that node is healthy
                    // We exclude 0 since some clients return 0 when request fails
                    this.setNodeHealthcheck(node, HEALTHY);
                }
                this.logger.debug(`Request #${requestNumber}: Request to Node ${node.index} was made. Response Code was ${response.status}.`);
                if (response.status >= 200 && response.status < 300) {
                    // If response is 2xx return a resolved promise
                    return Promise.resolve(response.data);
                }
                else if (response.status < 500) {
                    // Next, if response is anything but 5xx, don't retry, return a custom error
                    return Promise.reject(this.customErrorForResponse(response, (_a = response.data) === null || _a === void 0 ? void 0 : _a.message));
                }
                else {
                    // Retry all other HTTP errors (HTTPStatus > 500)
                    // This will get caught by the catch block below
                    throw this.customErrorForResponse(response, (_b = response.data) === null || _b === void 0 ? void 0 : _b.message);
                }
            }
            catch (error) {
                // This block handles retries for HTTPStatus > 500 and network layer issues like connection timeouts
                this.setNodeHealthcheck(node, UNHEALTHY);
                lastException = error;
                this.logger.warn(`Request #${requestNumber}: Request to Node ${node.index} failed due to "${error.code} ${error.message}${error.response == null ? '' : ' - ' + JSON.stringify((_c = error.response) === null || _c === void 0 ? void 0 : _c.data)}"`);
                // this.logger.debug(error.stack)
                this.logger.warn(`Request #${requestNumber}: Sleeping for ${this.retryIntervalSeconds}s and then retrying request...`);
                await this.timer(this.retryIntervalSeconds);
            }
            finally {
                if (abortSignal && abortListener) {
                    abortSignal.removeEventListener('abort', abortListener);
                }
            }
        }
        this.logger.debug(`Request #${requestNumber}: No retries left. Raising last error`);
        return Promise.reject(lastException);
    }
    // Attempts to find the next healthy node, looping through the list of nodes once.
    //   But if no healthy nodes are found, it will just return the next node, even if it's unhealthy
    //     so we can try the request for good measure, in case that node has become healthy since
    getNextNode(requestNumber = 0) {
        // Check if nearestNode is set and is healthy, if so return it
        if (this.nearestNode != null) {
            this.logger.debug(`Request #${requestNumber}: Nodes Health: Node ${this.nearestNode.index} is ${this.nearestNode.isHealthy === true ? 'Healthy' : 'Unhealthy'}`);
            if (this.nearestNode.isHealthy === true || this.nodeDueForHealthcheck(this.nearestNode, requestNumber)) {
                this.logger.debug(`Request #${requestNumber}: Updated current node to Node ${this.nearestNode.index}`);
                return this.nearestNode;
            }
            this.logger.debug(`Request #${requestNumber}: Falling back to individual nodes`);
        }
        // Fallback to nodes as usual
        this.logger.debug(`Request #${requestNumber}: Nodes Health: ${this.nodes
            .map((node) => `Node ${node.index} is ${node.isHealthy === true ? 'Healthy' : 'Unhealthy'}`)
            .join(' || ')}`);
        let candidateNode;
        for (let i = 0; i <= this.nodes.length; i++) {
            this.currentNodeIndex = (this.currentNodeIndex + 1) % this.nodes.length;
            candidateNode = this.nodes[this.currentNodeIndex];
            if (candidateNode.isHealthy === true || this.nodeDueForHealthcheck(candidateNode, requestNumber)) {
                this.logger.debug(`Request #${requestNumber}: Updated current node to Node ${candidateNode.index}`);
                return candidateNode;
            }
        }
        // None of the nodes are marked healthy, but some of them could have become healthy since last health check.
        //  So we will just return the next node.
        this.logger.debug(`Request #${requestNumber}: No healthy nodes were found. Returning the next node, Node ${candidateNode.index}`);
        return candidateNode;
    }
    nodeDueForHealthcheck(node, requestNumber = 0) {
        const isDueForHealthcheck = Date.now() - node.lastAccessTimestamp > this.healthcheckIntervalSeconds * 1000;
        if (isDueForHealthcheck) {
            this.logger.debug(`Request #${requestNumber}: Node ${node.index} has exceeded healtcheckIntervalSeconds of ${this.healthcheckIntervalSeconds}. Adding it back into rotation.`);
        }
        return isDueForHealthcheck;
    }
    initializeMetadataForNodes() {
        if (this.nearestNode != null) {
            this.nearestNode.index = 'nearestNode';
            this.setNodeHealthcheck(this.nearestNode, HEALTHY);
        }
        this.nodes.forEach((node, i) => {
            node.index = i;
            this.setNodeHealthcheck(node, HEALTHY);
        });
    }
    setNodeHealthcheck(node, isHealthy) {
        node.isHealthy = isHealthy;
        node.lastAccessTimestamp = Date.now();
    }
    uriFor(endpoint, node) {
        if (node.url != null) {
            return `${node.url}${endpoint}`;
        }
        return `${node.protocol}://${node.host}:${node.port}${node.path}${endpoint}`;
    }
    defaultHeaders() {
        let defaultHeaders = {};
        if (!this.sendApiKeyAsQueryParam) {
            defaultHeaders[APIKEYHEADERNAME] = this.apiKey;
        }
        defaultHeaders['Content-Type'] = 'application/json';
        return defaultHeaders;
    }
    async timer(seconds) {
        return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
    }
    customErrorForResponse(response, messageFromServer) {
        let errorMessage = `Request failed with HTTP code ${response.status}`;
        if (typeof messageFromServer === 'string' && messageFromServer.trim() !== '') {
            errorMessage += ` | Server said: ${messageFromServer}`;
        }
        let error = new TypesenseError_1.default(errorMessage);
        if (response.status === 400) {
            error = new Errors_1.RequestMalformed(errorMessage);
        }
        else if (response.status === 401) {
            error = new Errors_1.RequestUnauthorized(errorMessage);
        }
        else if (response.status === 404) {
            error = new Errors_1.ObjectNotFound(errorMessage);
        }
        else if (response.status === 409) {
            error = new Errors_1.ObjectAlreadyExists(errorMessage);
        }
        else if (response.status === 422) {
            error = new Errors_1.ObjectUnprocessable(errorMessage);
        }
        else if (response.status >= 500 && response.status <= 599) {
            error = new Errors_1.ServerError(errorMessage);
        }
        else {
            error = new Errors_1.HTTPError(errorMessage);
        }
        error.httpStatus = response.status;
        return error;
    }
}
exports.default = ApiCall;
//# sourceMappingURL=ApiCall.js.map