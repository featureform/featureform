"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const RESOURCEPATH = '/aliases';
class Aliases {
    constructor(apiCall) {
        this.apiCall = apiCall;
    }
    async upsert(name, mapping) {
        return await this.apiCall.put(this.endpointPath(name), mapping);
    }
    async retrieve() {
        return await this.apiCall.get(RESOURCEPATH);
    }
    endpointPath(aliasName) {
        return `${Aliases.RESOURCEPATH}/${aliasName}`;
    }
    static get RESOURCEPATH() {
        return RESOURCEPATH;
    }
}
exports.default = Aliases;
//# sourceMappingURL=Aliases.js.map