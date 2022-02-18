"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const RESOURCEPATH = '/operations';
class Operations {
    constructor(apiCall) {
        this.apiCall = apiCall;
    }
    perform(operationName, queryParameters = {}) {
        return this.apiCall.post(`${RESOURCEPATH}/${operationName}`, {}, queryParameters);
    }
}
exports.default = Operations;
//# sourceMappingURL=Operations.js.map