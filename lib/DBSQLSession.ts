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

const defaultMaxRows = 100000;

interface OperationResponseShape {
  status: TStatus;
  operationHandle?: TOperationHandle;
  directResults?: TSparkDirectResults;
}

function getDirectResultsOptions(maxRows: number | null = defaultMaxRows) {
  if (maxRows === null) {
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

  constructor(driver: HiveDriver, sessionHandle: TSessionHandle) {
    this.driver = driver;
    this.sessionHandle = sessionHandle;
    this.statusFactory = new StatusFactory();
  }

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

  getTypeInfo(request: TypeInfoRequest = {}): Promise<IOperation> {
    return this.driver
      .getTypeInfo({
        sessionHandle: this.sessionHandle,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

  getCatalogs(request: CatalogsRequest = {}): Promise<IOperation> {
    return this.driver
      .getCatalogs({
        sessionHandle: this.sessionHandle,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

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

  getTableTypes(request: TableTypesRequest = {}): Promise<IOperation> {
    return this.driver
      .getTableTypes({
        sessionHandle: this.sessionHandle,
        runAsync: request.runAsync || false,
        ...getDirectResultsOptions(request.maxRows),
      })
      .then((response) => this.createOperation(response));
  }

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

  close(): Promise<Status> {
    return this.driver
      .closeSession({
        sessionHandle: this.sessionHandle,
      })
      .then((response) => this.statusFactory.create(response.status));
  }

  private createOperation(response: OperationResponseShape): IOperation {
    this.assertStatus(response.status);
    const handle = definedOrError(response.operationHandle);
    return new DBSQLOperation(this.driver, handle, response.directResults);
  }

  private assertStatus(responseStatus: TStatus): void {
    this.statusFactory.create(responseStatus);
  }
}
