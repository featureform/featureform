"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Aliases_1 = __importDefault(require("./Aliases"));
class Alias {
    constructor(name, apiCall) {
        this.name = name;
        this.apiCall = apiCall;
    }
    async retrieve() {
        return await this.apiCall.get(this.endpointPath());
    }
    async delete() {
        return await this.apiCall.delete(this.endpointPath());
    }
    endpointPath() {
        return `${Aliases_1.default.RESOURCEPATH}/${this.name}`;
    }
}
exports.default = Alias;
//# sourceMappingURL=Alias.js.map