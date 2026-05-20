import IOperationBackend from './IOperationBackend';
import {
  ExecuteStatementOptions,
  TypeInfoRequest,
  CatalogsRequest,
  SchemasRequest,
  TablesRequest,
  TableTypesRequest,
  ColumnsRequest,
  FunctionsRequest,
  ProceduresRequest,
  PrimaryKeysRequest,
  CrossReferenceRequest,
} from './IDBSQLSession';
import Status from '../dto/Status';
import InfoValue from '../dto/InfoValue';

/**
 * What a `DBSQLSession` needs from its backend. Returned by
 * `IBackend.openSession()`. Lifecycle tied to a single `DBSQLSession`.
 */
export default interface ISessionBackend {
  readonly id: string;

  getInfo(infoType: number): Promise<InfoValue>;

  executeStatement(statement: string, options: ExecuteStatementOptions): Promise<IOperationBackend>;

  getTypeInfo(request: TypeInfoRequest): Promise<IOperationBackend>;
  getCatalogs(request: CatalogsRequest): Promise<IOperationBackend>;
  getSchemas(request: SchemasRequest): Promise<IOperationBackend>;
  getTables(request: TablesRequest): Promise<IOperationBackend>;
  getTableTypes(request: TableTypesRequest): Promise<IOperationBackend>;
  getColumns(request: ColumnsRequest): Promise<IOperationBackend>;
  getFunctions(request: FunctionsRequest): Promise<IOperationBackend>;
  getProcedures(request: ProceduresRequest): Promise<IOperationBackend>;
  getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperationBackend>;
  getCrossReference(request: CrossReferenceRequest): Promise<IOperationBackend>;

  close(): Promise<Status>;
}
