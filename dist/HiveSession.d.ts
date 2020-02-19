import HiveDriver from "./hive/HiveDriver";
import IHiveSession, { ExecuteStatementOptions, SchemasRequest, TablesRequest, ColumnRequest, PrimaryKeysRequest, FunctionNameRequest, CrossReferenceRequest } from './contracts/IHiveSession';
import { SessionHandle, TCLIServiceTypes } from "./hive/Types";
import IOperation from "./contracts/IOperation";
import Status from "./dto/Status";
import InfoValue from "./dto/InfoValue";
export default class HiveSession implements IHiveSession {
    private driver;
    private sessionHandle;
    private TCLIService_types;
    private statusFactory;
    constructor(driver: HiveDriver, sessionHandle: SessionHandle, TCLIService_types: TCLIServiceTypes);
    getInfo(infoType: number): Promise<InfoValue>;
    executeStatement(statement: string, options?: ExecuteStatementOptions): Promise<IOperation>;
    getTypeInfo(): Promise<IOperation>;
    getCatalogs(): Promise<IOperation>;
    getSchemas(request: SchemasRequest): Promise<IOperation>;
    getTables(request: TablesRequest): Promise<IOperation>;
    getTableTypes(): Promise<IOperation>;
    getColumns(request: ColumnRequest): Promise<IOperation>;
    getFunctions(request: FunctionNameRequest): Promise<IOperation>;
    getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation>;
    getCrossReference(request: CrossReferenceRequest): Promise<IOperation>;
    getDelegationToken(owner: string, renewer: string): Promise<string>;
    renewDelegationToken(token: string): Promise<Status>;
    cancelDelegationToken(token: string): Promise<Status>;
    close(): Promise<Status>;
    private createOperation;
    private assertStatus;
}
