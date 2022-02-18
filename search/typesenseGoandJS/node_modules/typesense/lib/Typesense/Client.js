"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Configuration_1 = __importDefault(require("./Configuration"));
const ApiCall_1 = __importDefault(require("./ApiCall"));
const Collections_1 = __importDefault(require("./Collections"));
const Collection_1 = __importDefault(require("./Collection"));
const Aliases_1 = __importDefault(require("./Aliases"));
const Alias_1 = __importDefault(require("./Alias"));
const Keys_1 = __importDefault(require("./Keys"));
const Key_1 = __importDefault(require("./Key"));
const Debug_1 = __importDefault(require("./Debug"));
const Metrics_1 = __importDefault(require("./Metrics"));
const Health_1 = __importDefault(require("./Health"));
const Operations_1 = __importDefault(require("./Operations"));
const MultiSearch_1 = __importDefault(require("./MultiSearch"));
class Client {
    constructor(options) {
        this.configuration = new Configuration_1.default(options);
        this.apiCall = new ApiCall_1.default(this.configuration);
        this.debug = new Debug_1.default(this.apiCall);
        this.metrics = new Metrics_1.default(this.apiCall);
        this.health = new Health_1.default(this.apiCall);
        this.operations = new Operations_1.default(this.apiCall);
        this.multiSearch = new MultiSearch_1.default(this.apiCall, this.configuration);
        this._collections = new Collections_1.default(this.apiCall);
        this.individualCollections = {};
        this._aliases = new Aliases_1.default(this.apiCall);
        this.individualAliases = {};
        this._keys = new Keys_1.default(this.apiCall);
        this.individualKeys = {};
    }
    collections(collectionName) {
        if (collectionName === undefined) {
            return this._collections;
        }
        else {
            if (this.individualCollections[collectionName] === undefined) {
                this.individualCollections[collectionName] = new Collection_1.default(collectionName, this.apiCall, this.configuration);
            }
            return this.individualCollections[collectionName];
        }
    }
    aliases(aliasName) {
        if (aliasName === undefined) {
            return this._aliases;
        }
        else {
            if (this.individualAliases[aliasName] === undefined) {
                this.individualAliases[aliasName] = new Alias_1.default(aliasName, this.apiCall);
            }
            return this.individualAliases[aliasName];
        }
    }
    keys(id) {
        if (id === undefined) {
            return this._keys;
        }
        else {
            if (this.individualKeys[id] === undefined) {
                this.individualKeys[id] = new Key_1.default(id, this.apiCall);
            }
            return this.individualKeys[id];
        }
    }
}
exports.default = Client;
//# sourceMappingURL=Client.js.map