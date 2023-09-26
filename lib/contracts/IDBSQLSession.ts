import IOperation from './IOperation';
import Status from '../dto/Status';
import InfoValue from '../dto/InfoValue';
import { Int64 } from '../hive/Types';
import { DBSQLParameter, DBSQLParameterValue } from '../DBSQLParameter';

export type ExecuteStatementOptions = {
  queryTimeout?: Int64;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | null;
  useCloudFetch?: boolean;
  stagingAllowedLocalPath?: string | string[];
  namedParameters?: Record<string, DBSQLParameter | DBSQLParameterValue>;
  ordinalParameters?: Array<DBSQLParameter | DBSQLParameterValue>;
};

export type TypeInfoRequest = {
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | null;
};

export type CatalogsRequest = {
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | null;
};

export type SchemasRequest = {
  catalogName?: string;
  schemaName?: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | null;
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
  maxRows?: number | null;
};

export type TableTypesRequest = {
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | null;
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
  maxRows?: number | null;
};

export type FunctionsRequest = {
  catalogName?: string;
  schemaName?: string;
  functionName: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | null;
};

export type PrimaryKeysRequest = {
  catalogName?: string;
  schemaName: string;
  tableName: string;
  /**
   * @deprecated This option is no longer supported and will be removed in future releases
   */
  runAsync?: boolean;
  maxRows?: number | null;
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
  maxRows?: number | null;
};

export default interface IDBSQLSession {
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
