import Int64 from 'node-int64';
import IOperation from './IOperation';
import Status from '../dto/Status';
import InfoValue from '../dto/InfoValue';
import { DBSQLParameter, DBSQLParameterValue } from '../DBSQLParameter';

export type ExecuteStatementOptions = {
  /**
   * The number of seconds after which the query will time out on the server.
   * Effective only with Compute clusters. For SQL Warehouses, `STATEMENT_TIMEOUT`
   * configuration should be used
   */
  queryTimeout?: number | bigint | Int64;
  /**
   * Selects the execution lifecycle. The only observable effect is WHEN
   * `executeStatement` resolves; the result data, schema, and error classes are
   * identical regardless.
   *
   * - **Thrift backend:** no-op. The Thrift path always submits asynchronously
   *   (`runAsync: true` on the wire) and polls during fetch; this option is not
   *   read.
   * - **Kernel backend (`useKernel`):** selects the kernel execution path —
   *   `false`/unset (default) runs the blocking direct-results path (faster,
   *   cancellable mid-compute); `true` submits and polls (returns a pending
   *   handle before completion). Default is sync, matching the python
   *   connector's `cursor.execute()`.
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
  useCloudFetch?: boolean;
  useLZ4Compression?: boolean;
  stagingAllowedLocalPath?: string | string[];
  namedParameters?: Record<string, DBSQLParameter | DBSQLParameterValue>;
  ordinalParameters?: Array<DBSQLParameter | DBSQLParameterValue>;
  /**
   * Per-statement query tags as key-value pairs. Serialized and passed via confOverlay
   * as "query_tags". Values may be null/undefined to include a key without a value.
   * These tags apply only to this statement and do not persist across queries.
   */
  queryTags?: Record<string, string | null | undefined>;
  /**
   * kernel-only: server-side row cap for this statement (kernel `row_limit`). The
   * Thrift backend has no execute-time server cap, so this is a no-op there;
   * use `maxRows` for the cross-backend client-side fetch limit.
   */
  rowLimit?: number;
  /**
   * kernel-only: per-statement Spark conf overlay (kernel `statement_conf`).
   * Merged with the serialized `queryTags` (which land under the reserved
   * `query_tags` key). Ignored by the Thrift backend.
   */
  statementConf?: Record<string, string>;
};

export type TypeInfoRequest = {
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type CatalogsRequest = {
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type SchemasRequest = {
  catalogName?: string;
  schemaName?: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type TablesRequest = {
  catalogName?: string;
  schemaName?: string;
  tableName?: string;
  tableTypes?: Array<string>;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type TableTypesRequest = {
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type ColumnsRequest = {
  catalogName?: string;
  schemaName?: string;
  tableName?: string;
  columnName?: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type FunctionsRequest = {
  catalogName?: string;
  schemaName?: string;
  functionName: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type PrimaryKeysRequest = {
  catalogName?: string;
  schemaName: string;
  tableName: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export type CrossReferenceRequest = {
  parentCatalogName: string;
  parentSchemaName: string;
  parentTableName: string;
  foreignCatalogName: string;
  foreignSchemaName: string;
  foreignTableName: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | bigint | Int64 | null;
};

export default interface IDBSQLSession {
  /**
   * Session identifier
   */
  readonly id: string;

  /**
   * Returns general information about the data source
   *
   * @param infoType one of the values TCLIService_types.TGetInfoType
   */
  getInfo(infoType: number): Promise<InfoValue>;

  /**
   * Executes DDL/DML statements
   *
   * @param statement DDL/DML statement
   * @param options
   */
  executeStatement(statement: string, options?: ExecuteStatementOptions): Promise<IOperation>;

  /**
   * Information about supported data types
   *
   * @param request
   */
  getTypeInfo(request?: TypeInfoRequest): Promise<IOperation>;

  /**
   * Get list of catalogs
   *
   * @param request
   */
  getCatalogs(request?: CatalogsRequest): Promise<IOperation>;

  /**
   * Get list of databases
   *
   * @param request
   */
  getSchemas(request?: SchemasRequest): Promise<IOperation>;

  /**
   * Get list of tables
   *
   * @param request
   */
  getTables(request?: TablesRequest): Promise<IOperation>;

  /**
   * Get list of supported table types
   *
   * @param request
   */
  getTableTypes(request?: TableTypesRequest): Promise<IOperation>;

  /**
   * Get full information about columns of the table
   *
   * @param request
   */
  getColumns(request?: ColumnsRequest): Promise<IOperation>;

  /**
   * Get information about function
   *
   * @param request
   */
  getFunctions(request: FunctionsRequest): Promise<IOperation>;

  /**
   * Get primary keys of table
   *
   * @param request
   */
  getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation>;

  /**
   * Request information about foreign keys between two tables
   * @param request
   */
  getCrossReference(request: CrossReferenceRequest): Promise<IOperation>;

  /**
   * closes the session
   */
  close(): Promise<Status>;
}
