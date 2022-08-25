import { TSessionHandle, TStatus, TOperationHandle } from '../thrift/TCLIService_types';
import HiveDriver from './hive/HiveDriver';
import IDBSQLSession, {
  ExecuteStatementOptions,
  SchemasRequest,
  TablesRequest,
  ColumnRequest,
  PrimaryKeysRequest,
  FunctionNameRequest,
  CrossReferenceRequest,
} from './contracts/IDBSQLSession';
import IOperation from './contracts/IOperation';
import DBSQLOperation from './DBSQLOperation';
import Status from './dto/Status';
import StatusFactory from './factory/StatusFactory';
import InfoValue from './dto/InfoValue';
import { definedOrError } from './utils';
import formatQuery from './utils/formatQuery';

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
    options = {
      runAsync: false,
      ...options,
    };

    if (options.queryParams) {
      statement = formatQuery(statement, options.queryParams);
    }

    return this.driver
      .executeStatement({
        sessionHandle: this.sessionHandle,
        statement,
        ...options,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getTypeInfo(): Promise<IOperation> {
    return this.driver
      .getTypeInfo({
        sessionHandle: this.sessionHandle,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getCatalogs(): Promise<IOperation> {
    return this.driver
      .getCatalogs({
        sessionHandle: this.sessionHandle,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getSchemas(request: SchemasRequest): Promise<IOperation> {
    return this.driver
      .getSchemas({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getTables(request: TablesRequest): Promise<IOperation> {
    return this.driver
      .getTables({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        tableName: request.tableName,
        tableTypes: request.tableTypes,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getTableTypes(): Promise<IOperation> {
    return this.driver
      .getTableTypes({
        sessionHandle: this.sessionHandle,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getColumns(request: ColumnRequest): Promise<IOperation> {
    return this.driver
      .getColumns({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        tableName: request.tableName,
        columnName: request.columnName,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getFunctions(request: FunctionNameRequest): Promise<IOperation> {
    return this.driver
      .getFunctions({
        sessionHandle: this.sessionHandle,
        functionName: request.functionName,
        schemaName: request.schemaName,
        catalogName: request.catalogName,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
  }

  getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation> {
    return this.driver
      .getPrimaryKeys({
        sessionHandle: this.sessionHandle,
        catalogName: request.catalogName,
        schemaName: request.schemaName,
        tableName: request.tableName,
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
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
      })
      .then((response) => {
        this.assertStatus(response.status);

        return this.createOperation(definedOrError(response.operationHandle));
      });
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

  private createOperation(handle: TOperationHandle): IOperation {
    return new DBSQLOperation(this.driver, handle);
  }

  private assertStatus(responseStatus: TStatus): void {
    this.statusFactory.create(responseStatus);
  }
}
