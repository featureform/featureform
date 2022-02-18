"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = require("crypto");
const RESOURCEPATH = '/keys';
class Keys {
    constructor(apiCall) {
        this.apiCall = apiCall;
        this.apiCall = apiCall;
    }
    async create(params) {
        return await this.apiCall.post(Keys.RESOURCEPATH, params);
    }
    retrieve() {
        return this.apiCall.get(RESOURCEPATH);
    }
    generateScopedSearchKey(searchKey, parameters) {
        // Note: only a key generated with the `documents:search` action will be
        // accepted by the server, when usined with the search endpoint.
        const paramsJSON = JSON.stringify(parameters);
        const digest = Buffer.from(crypto_1.createHmac('sha256', searchKey).update(paramsJSON).digest('base64'));
        const keyPrefix = searchKey.substr(0, 4);
        const rawScopedKey = `${digest}${keyPrefix}${paramsJSON}`;
        return Buffer.from(rawScopedKey).toString('base64');
    }
    static get RESOURCEPATH() {
        return RESOURCEPATH;
    }
}
exports.default = Keys;
//# sourceMappingURL=Keys.js.map