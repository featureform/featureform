import { CollectionAliasSchema } from './Aliases';
import ApiCall from './ApiCall';
export default class Alias {
    private name;
    private apiCall;
    constructor(name: string, apiCall: ApiCall);
    retrieve(): Promise<CollectionAliasSchema>;
    delete(): Promise<CollectionAliasSchema>;
    private endpointPath;
}
