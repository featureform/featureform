"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Collections_1 = __importDefault(require("./Collections"));
const Overrides_1 = __importDefault(require("./Overrides"));
class Override {
    constructor(collectionName, overrideId, apiCall) {
        this.collectionName = collectionName;
        this.overrideId = overrideId;
        this.apiCall = apiCall;
    }
    async retrieve() {
        return await this.apiCall.get(this.endpointPath());
    }
    async delete() {
        return await this.apiCall.delete(this.endpointPath());
    }
    endpointPath() {
        return `${Collections_1.default.RESOURCEPATH}/${this.collectionName}${Overrides_1.default.RESOURCEPATH}/${this.overrideId}`;
    }
}
exports.default = Override;
//# sourceMappingURL=Override.js.map