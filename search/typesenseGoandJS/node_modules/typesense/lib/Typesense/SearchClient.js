"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Configuration_1 = __importDefault(require("./Configuration"));
const ApiCall_1 = __importDefault(require("./ApiCall"));
const MultiSearch_1 = __importDefault(require("./MultiSearch"));
const SearchOnlyCollection_1 = require("./SearchOnlyCollection");
class SearchClient {
    constructor(options) {
        const shouldSendApiKeyAsQueryParam = (options['apiKey'] || '').length < 2000;
        if (shouldSendApiKeyAsQueryParam) {
            options['sendApiKeyAsQueryParam'] = true;
        }
        this.configuration = new Configuration_1.default(options);
        this.apiCall = new ApiCall_1.default(this.configuration);
        this.multiSearch = new MultiSearch_1.default(this.apiCall, this.configuration, true);
        this.individualCollections = {};
    }
    collections(collectionName) {
        if (!collectionName) {
            throw new Error('Typesense.SearchClient only supports search operations, so the collectionName that needs to ' +
                'be searched must be specified. Use Typesense.Client if you need to access the collection object.');
        }
        else {
            if (this.individualCollections[collectionName] === undefined) {
                this.individualCollections[collectionName] = new SearchOnlyCollection_1.SearchOnlyCollection(collectionName, this.apiCall, this.configuration);
            }
            return this.individualCollections[collectionName];
        }
    }
}
exports.default = SearchClient;
//# sourceMappingURL=SearchClient.js.map