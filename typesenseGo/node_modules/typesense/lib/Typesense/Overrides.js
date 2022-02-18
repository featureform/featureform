"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Collections_1 = __importDefault(require("./Collections"));
const RESOURCEPATH = '/overrides';
class Overrides {
    constructor(collectionName, apiCall) {
        this.collectionName = collectionName;
        this.apiCall = apiCall;
    }
    async upsert(overrideId, params) {
        return await this.apiCall.put(this.endpointPath(overrideId), params);
    }
    async retrieve() {
        return await this.apiCall.get(this.endpointPath());
    }
    endpointPath(operation) {
        return `${Collections_1.default.RESOURCEPATH}/${this.collectionName}${Overrides.RESOURCEPATH}${operation === undefined ? '' : '/' + operation}`;
    }
    static get RESOURCEPATH() {
        return RESOURCEPATH;
    }
}
exports.default = Overrides;
//# sourceMappingURL=Overrides.js.map