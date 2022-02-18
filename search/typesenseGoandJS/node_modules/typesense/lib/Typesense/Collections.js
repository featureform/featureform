"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const RESOURCEPATH = '/collections';
class Collections {
    constructor(apiCall) {
        this.apiCall = apiCall;
    }
    async create(schema) {
        return await this.apiCall.post(RESOURCEPATH, schema);
    }
    async retrieve() {
        return await this.apiCall.get(RESOURCEPATH);
    }
    static get RESOURCEPATH() {
        return RESOURCEPATH;
    }
}
exports.default = Collections;
//# sourceMappingURL=Collections.js.map