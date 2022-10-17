import { stringify, NIL, parse } from 'uuid';
import { TSessionHandle, TStatus, TOperationHandle, TSparkDirectResults } from '../thrift/TCLIService_types';
import HiveDriver from './hive/HiveDriver';
import { Int64 } from './hive/Types';
import IDBSQLSession, {
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
} from './contracts/IDBSQLSession';
import IOperation from './contracts/IOperation';
import DBSQLOperation from './DBSQLOperation';
import Status from './dto/Status';
import StatusFactory from './factory/StatusFactory';
import InfoValue from './dto/InfoValue';
import { definedOrError } from './utils';
import IDBSQLLogger, { LOGLEVEL } from './contracts/IDBSQLLogger';
import DBSQLLogger from './DBSQLLogger';

interface OperationResponseShape {
  status: TStatus;
  operationHandle?: TOperationHandle;
  directResults?: TSparkDirectResults;
}

function getDirectResultsOptions(maxRows?: number) {
  if (!maxRows) {
    return {};
  }

  return {
    getDirectResults: {
      maxRows: new Int64(maxRows),
    },
  };
}

export default class DBSQLSession implements IDBSQLSession {
  private driver: HiveDriver;

  private sessionHandle: TSessionHandle;

  private statusFactory: StatusFactory;

  private logger: IDBSQLLogger;

  constructor(driver: HiveDriver, sessionHandle: TSessionHandle, logger?: IDBSQLLogger) {
    this.driver = driver;
    this.sessionHandle = sessionHandle;
    this.statusFactory = new StatusFactory();
    this.logger = logger || new DBSQLLogger();
    this.logger.log(
      LOGLEVEL.debug,
      `Session created with id: ${stringify(this.sessionHandle?.sessionId?.guid || parse(NIL))}`,
    );
  }

  /**
   * Fetches info
   * @public
   * @param infoType - One of the values TCLIService_types.TGetInfoType
   * @returns Value corresponding to info type requested
   * @example
   * const response = await session.getInfo(thrift.TCLIService_types.TGetInfoType.CLI_DBMS_VER);
   */
  getInfo(infoType: number): Promise<InfoValue> {
    return this.driver
      .getInfo({
        sessionHandle: this.sessionHandle,
        infoType,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return new InfoValue(response.infoValue);
      });
  }

  /**
   * Executes statement
   * @public
   * @param statement - SQL statement to be executed
   * @param options - maxRows field is used to specify Direct Results
   * @returns DBSQLOperation
   * @example
   * const operation = await session.executeStatement(query, { runAsync: true });
   */
  executeStatement(statement: string, options: ExecuteStatementOptions = {}): Promise<IOperation> {
    return this.driver
      .executeStatement({
        sessionHandle: this.sessionHandle,
        statement,
        queryTimeout: options.queryTimeout,
        runAsync: options.runAsync || false,
        ...getDirectResultsOptions(options.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Information about supported data types
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getTypeInfo(request: TypeInfoRequest = {}): Promise<IOperation> {
    return this.driver
      .getTypeInfo({
        sessionHandle: this.sessionHandle,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Get list of catalogs
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getCatalogs(request: CatalogsRequest = {}): Promise<IOperation> {
    return this.driver
      .getCatalogs({
        sessionHandle: this.sessionHandle,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Get list of schemas
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getSchemas(request: SchemasRequest = {}): Promise<IOperation> {
    return this.driver
      .getSchemas({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Get list of tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getTables(request: TablesRequest = {}): Promise<IOperation> {
    return this.driver
      .getTables({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        tableName: request.tableName,
        tableTypes: request.tableTypes,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Get list of supported table types
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getTableTypes(request: TableTypesRequest = {}): Promise<IOperation> {
    return this.driver
      .getTableTypes({
        sessionHandle: this.sessionHandle,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Get full information about columns of the table
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getColumns(request: ColumnsRequest = {}): Promise<IOperation> {
    return this.driver
      .getColumns({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        tableName: request.tableName,
        columnName: request.columnName,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Get information about function
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getFunctions(request: FunctionsRequest): Promise<IOperation> {
    return this.driver
      .getFunctions({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        functionName: request.functionName,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation> {
    return this.driver
      .getPrimaryKeys({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        tableName: request.tableName,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Request information about foreign keys between two tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  getCrossReference(request: CrossReferenceRequest): Promise<IOperation> {
    return this.driver
      .getCrossReference({
        sessionHandle: this.sessionHandle,
        parentCatalogName: request.parentCatalogName,
        parentSchemaName: request.parentSchemaName,
        parentTableName: request.parentTableName,
        foreignCatalogName: request.foreignCatalogName,
        foreignSchemaName: request.foreignSchemaName,
        foreignTableName: request.foreignTableName,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  /**
   * Get delegation token. For kerberos auth only
   * @public
   * @param owner
   * @param renewer
   * @returns Delegation token
   */
  getDelegationToken(owner: string, renewer: string): Promise<string> {
    return this.driver
      .getDelegationToken({
        sessionHandle: this.sessionHandle,
        owner,
        renewer,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return response.delegationToken || '';
      });
  }

  /**
   * Renew delegation token/ For kerberos auth only
   * @public
   * @param token
   * @returns Operation status
   */
  renewDelegationToken(token: string): Promise<Status> {
    return this.driver
      .renewDelegationToken({
        sessionHandle: this.sessionHandle,
        delegationToken: token,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.statusFactory.create(response.status);
      });
  }

  /**
   * Cancel delegation token. For kerberos auth only
   * @public
   * @param token
   * @returns Operation status
   */
  cancelDelegationToken(token: string): Promise<Status> {
    return this.driver
      .cancelDelegationToken({
        sessionHandle: this.sessionHandle,
        delegationToken: token,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.statusFactory.create(response.status);
      });
  }

  /**
   * Closes the session
   * @public
   * @returns Operation status
   */
  close(): Promise<Status> {
    return this.driver
      .closeSession({
        sessionHandle: this.sessionHandle,
      })
      .then((response) => {
        this.logger.log(
          LOGLEVEL.debug,
          `Session closed with id: ${stringify(this.sessionHandle?.sessionId?.guid || parse(NIL))}`,
        );
        return this.statusFactory.create(response.status);
      });
  }

  private createOperation(response: OperationResponseShape): IOperation {
    this.assertStatus(response.status);
    const handle = definedOrError(response.operationHandle);
    return new DBSQLOperation(this.driver, handle, response.directResults, this.logger);
  }

  private assertStatus(responseStatus: TStatus): void {
    this.statusFactory.create(responseStatus);
  }
}
