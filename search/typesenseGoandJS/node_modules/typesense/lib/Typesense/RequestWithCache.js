"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const defaultCacheResponseForSeconds = 2 * 60;
const defaultMaxSize = 100;
class RequestWithCache {
    constructor() {
        this.responseCache = new Map();
    }
    // Todo: should probably be passed a callback instead, or an apiCall instance. Types are messy this way
    async perform(requestContext, requestFunction, requestFunctionArguments, cacheOptions) {
        const { cacheResponseForSeconds = defaultCacheResponseForSeconds, maxSize = defaultMaxSize } = cacheOptions;
        const isCacheDisabled = cacheResponseForSeconds <= 0 || maxSize <= 0;
        if (isCacheDisabled) {
            return requestFunction.call(requestContext, ...requestFunctionArguments);
        }
        const requestFunctionArgumentsJSON = JSON.stringify(requestFunctionArguments);
        const cacheEntry = this.responseCache.get(requestFunctionArgumentsJSON);
        const now = Date.now();
        if (cacheEntry) {
            const isEntryValid = now - cacheEntry.requestTimestamp < cacheResponseForSeconds * 1000;
            if (isEntryValid) {
                this.responseCache.delete(requestFunctionArgumentsJSON);
                this.responseCache.set(requestFunctionArgumentsJSON, cacheEntry);
                return Promise.resolve(cacheEntry.response);
            }
            else {
                this.responseCache.delete(requestFunctionArgumentsJSON);
            }
        }
        const response = await requestFunction.call(requestContext, ...requestFunctionArguments);
        this.responseCache.set(requestFunctionArgumentsJSON, {
            requestTimestamp: now,
            response
        });
        const isCacheOverMaxSize = this.responseCache.size > maxSize;
        if (isCacheOverMaxSize) {
            const oldestEntry = this.responseCache.keys().next().value;
            this.responseCache.delete(oldestEntry);
        }
        return response;
    }
}
exports.default = RequestWithCache;
//# sourceMappingURL=RequestWithCache.js.map