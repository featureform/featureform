"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const TypesenseError_1 = __importDefault(require("./TypesenseError"));
class ImportError extends TypesenseError_1.default {
    constructor(message, importResults) {
        super();
        this.importResults = importResults;
    }
}
exports.default = ImportError;
//# sourceMappingURL=ImportError.js.map