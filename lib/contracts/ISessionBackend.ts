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
  /** Session identifier. */
  readonly id: string;

  /** Returns general information about the data source. */
  getInfo(infoType: number): Promise<InfoValue>;

  /** Executes DDL/DML statements. */
  executeStatement(statement: string, options: ExecuteStatementOptions): Promise<IOperationBackend>;

  /** Information about supported data types. */
  getTypeInfo(request: TypeInfoRequest): Promise<IOperationBackend>;

  /** List of catalogs. */
  getCatalogs(request: CatalogsRequest): Promise<IOperationBackend>;

  /** List of schemas. */
  getSchemas(request: SchemasRequest): Promise<IOperationBackend>;

  /** List of tables. */
  getTables(request: TablesRequest): Promise<IOperationBackend>;

  /** List of supported table types. */
  getTableTypes(request: TableTypesRequest): Promise<IOperationBackend>;

  /** Full column information for a table. */
  getColumns(request: ColumnsRequest): Promise<IOperationBackend>;

  /** Information about a function. */
  getFunctions(request: FunctionsRequest): Promise<IOperationBackend>;

  /** Primary keys of a table. */
  getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperationBackend>;

  /** Foreign-key relationships between two tables. */
  getCrossReference(request: CrossReferenceRequest): Promise<IOperationBackend>;

  /** Close the session. Idempotent. */
  close(): Promise<Status>;
}
