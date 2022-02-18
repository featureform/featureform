"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ImportError = exports.TypesenseError = exports.ServerError = exports.RequestUnauthorized = exports.RequestMalformed = exports.ObjectUnprocessable = exports.ObjectNotFound = exports.ObjectAlreadyExists = exports.MissingConfigurationError = exports.HTTPError = void 0;
const HTTPError_1 = __importDefault(require("./HTTPError"));
exports.HTTPError = HTTPError_1.default;
const MissingConfigurationError_1 = __importDefault(require("./MissingConfigurationError"));
exports.MissingConfigurationError = MissingConfigurationError_1.default;
const ObjectAlreadyExists_1 = __importDefault(require("./ObjectAlreadyExists"));
exports.ObjectAlreadyExists = ObjectAlreadyExists_1.default;
const ObjectNotFound_1 = __importDefault(require("./ObjectNotFound"));
exports.ObjectNotFound = ObjectNotFound_1.default;
const ObjectUnprocessable_1 = __importDefault(require("./ObjectUnprocessable"));
exports.ObjectUnprocessable = ObjectUnprocessable_1.default;
const RequestMalformed_1 = __importDefault(require("./RequestMalformed"));
exports.RequestMalformed = RequestMalformed_1.default;
const RequestUnauthorized_1 = __importDefault(require("./RequestUnauthorized"));
exports.RequestUnauthorized = RequestUnauthorized_1.default;
const ServerError_1 = __importDefault(require("./ServerError"));
exports.ServerError = ServerError_1.default;
const ImportError_1 = __importDefault(require("./ImportError"));
exports.ImportError = ImportError_1.default;
const TypesenseError_1 = __importDefault(require("./TypesenseError"));
exports.TypesenseError = TypesenseError_1.default;
//# sourceMappingURL=index.js.map