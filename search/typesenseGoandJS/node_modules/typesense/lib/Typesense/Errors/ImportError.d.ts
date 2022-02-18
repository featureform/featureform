import TypesenseError from './TypesenseError';
import { ImportResponseFail } from '../Documents';
export default class ImportError extends TypesenseError {
    importResults: ImportResponseFail;
    constructor(message: any, importResults: any);
}
