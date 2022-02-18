"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Collections_1 = __importDefault(require("./Collections"));
const Synonyms_1 = __importDefault(require("./Synonyms"));
class Synonym {
    constructor(collectionName, synonymId, apiCall) {
        this.collectionName = collectionName;
        this.synonymId = synonymId;
        this.apiCall = apiCall;
    }
    async retrieve() {
        return await this.apiCall.get(this.endpointPath());
    }
    async delete() {
        return await this.apiCall.delete(this.endpointPath());
    }
    endpointPath() {
        return `${Collections_1.default.RESOURCEPATH}/${this.collectionName}${Synonyms_1.default.RESOURCEPATH}/${this.synonymId}`;
    }
}
exports.default = Synonym;
//# sourceMappingURL=Synonym.js.map