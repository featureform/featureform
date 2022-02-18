"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Keys_1 = __importDefault(require("./Keys"));
class Key {
    constructor(id, apiCall) {
        this.id = id;
        this.apiCall = apiCall;
    }
    async retrieve() {
        return await this.apiCall.get(this.endpointPath());
    }
    async delete() {
        return await this.apiCall.delete(this.endpointPath());
    }
    endpointPath() {
        return `${Keys_1.default.RESOURCEPATH}/${this.id}`;
    }
}
exports.default = Key;
//# sourceMappingURL=Key.js.map