"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const RESOURCEPATH = '/metrics.json';
class Metrics {
    constructor(apiCall) {
        this.apiCall = apiCall;
    }
    retrieve() {
        return this.apiCall.get(RESOURCEPATH);
    }
}
exports.default = Metrics;
//# sourceMappingURL=Metrics.js.map