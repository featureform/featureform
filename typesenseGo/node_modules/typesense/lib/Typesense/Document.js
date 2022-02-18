"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Document = void 0;
const Collections_1 = __importDefault(require("./Collections"));
const Documents_1 = __importDefault(require("./Documents"));
class Document {
    constructor(collectionName, documentId, apiCall) {
        this.collectionName = collectionName;
        this.documentId = documentId;
        this.apiCall = apiCall;
    }
    async retrieve() {
        return await this.apiCall.get(this.endpointPath());
    }
    async delete() {
        return await this.apiCall.delete(this.endpointPath());
    }
    async update(partialDocument, options = {}) {
        return await this.apiCall.patch(this.endpointPath(), partialDocument, options);
    }
    endpointPath() {
        return `${Collections_1.default.RESOURCEPATH}/${this.collectionName}${Documents_1.default.RESOURCEPATH}/${this.documentId}`;
    }
}
exports.Document = Document;
//# sourceMappingURL=Document.js.map