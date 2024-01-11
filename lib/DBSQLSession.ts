import * as fs from 'fs';
import * as path from 'path';
import { stringify, NIL, parse } from 'uuid';
import fetch, { HeadersInit } from 'node-fetch';
import {
  TSessionHandle,
  TStatus,
  TOperationHandle,
  TSparkDirectResults,
  TSparkArrowTypes,
  TSparkParameter,
} from '../thrift/TCLIService_types';
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
import InfoValue from './dto/InfoValue';
import { definedOrError } from './utils';
import CloseableCollection from './utils/CloseableCollection';
import { LogLevel } from './contracts/IDBSQLLogger';
import HiveDriverError from './errors/HiveDriverError';
import StagingError from './errors/StagingError';
import { DBSQLParameter, DBSQLParameterValue } from './DBSQLParameter';
import ParameterError from './errors/ParameterError';
import IClientContext, { ClientConfig } from './contracts/IClientContext';

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

function getArrowOptions(config: ClientConfig): {
  canReadArrowResult: boolean;
  useArrowNativeTypes?: TSparkArrowTypes;
} {
  const { arrowEnabled = true, useArrowNativeTypes = true } = config;

  if (!arrowEnabled) {
    return {
      canReadArrowResult: false,
    };
  }

  return {
    canReadArrowResult: true,
    useArrowNativeTypes: {
      timestampAsArrow: useArrowNativeTypes,
      decimalAsArrow: useArrowNativeTypes,
      complexTypesAsArrow: useArrowNativeTypes,
      // TODO: currently unsupported by `apache-arrow` (see https://github.com/streamlit/streamlit/issues/4489)
      intervalTypesAsArrow: false,
    },
  };
}

function getQueryParameters(
  sessionHandle: TSessionHandle,
  namedParameters?: Record<string, DBSQLParameter | DBSQLParameterValue>,
  ordinalParameters?: Array<DBSQLParameter | DBSQLParameterValue>,
): Array<TSparkParameter> {
  const namedParametersProvided = namedParameters !== undefined && Object.keys(namedParameters).length > 0;
  const ordinalParametersProvided = ordinalParameters !== undefined && ordinalParameters.length > 0;

  if (namedParametersProvided && ordinalParametersProvided) {
    throw new ParameterError('Driver does not support both ordinal and named parameters.');
  }

  if (!namedParametersProvided && !ordinalParametersProvided) {
    return [];
  }

  const result: Array<TSparkParameter> = [];

  if (namedParameters !== undefined) {
    for (const name of Object.keys(namedParameters)) {
      const value = namedParameters[name];
      const param = value instanceof DBSQLParameter ? value : new DBSQLParameter({ value });
      result.push(param.toSparkParameter({ name }));
    }
  }

  if (ordinalParameters !== undefined) {
    for (const value of ordinalParameters) {
      const param = value instanceof DBSQLParameter ? value : new DBSQLParameter({ value });
      result.push(param.toSparkParameter());
    }
  }

  return result;
}

interface DBSQLSessionConstructorOptions {
  handle: TSessionHandle;
  context: IClientContext;
}

export default class DBSQLSession implements IDBSQLSession {
  private readonly context: IClientContext;

  private readonly sessionHandle: TSessionHandle;

  private isOpen = true;

  public onClose?: () => void;

  private operations = new CloseableCollection<DBSQLOperation>();

  constructor({ handle, context }: DBSQLSessionConstructorOptions) {
    this.sessionHandle = handle;
    this.context = context;
    this.context.getLogger().log(LogLevel.debug, `Session created with id: ${this.getId()}`);
  }

  public getId() {
    return stringify(this.sessionHandle?.sessionId?.guid || parse(NIL));
  }

  /**
   * Fetches info
   * @public
   * @param infoType - One of the values TCLIService_types.TGetInfoType
   * @returns Value corresponding to info type requested
   * @example
   * const response = await session.getInfo(thrift.TCLIService_types.TGetInfoType.CLI_DBMS_VER);
   */
  public async getInfo(infoType: number): Promise<InfoValue> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getInfo({
      sessionHandle: this.sessionHandle,
      infoType,
    });
    const response = await this.handleResponse(operationPromise);
    Status.assert(response.status);
    return new InfoValue(response.infoValue);
  }

  /**
   * Executes statement
   * @public
   * @param statement - SQL statement to be executed
   * @param options - maxRows field is used to specify Direct Results
   * @returns DBSQLOperation
   * @example
   * const operation = await session.executeStatement(query);
   */
  public async executeStatement(statement: string, options: ExecuteStatementOptions = {}): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const clientConfig = this.context.getConfig();
    const operationPromise = driver.executeStatement({
      sessionHandle: this.sessionHandle,
      statement,
      queryTimeout: options.queryTimeout,
      runAsync: true,
      ...getDirectResultsOptions(options.maxRows),
      ...getArrowOptions(clientConfig),
      canDownloadResult: options.useCloudFetch ?? clientConfig.useCloudFetch,
      parameters: getQueryParameters(this.sessionHandle, options.namedParameters, options.ordinalParameters),
      canDecompressLZ4Result: clientConfig.useResultsCompression,
    });
    const response = await this.handleResponse(operationPromise);
    const operation = this.createOperation(response);

    // If `stagingAllowedLocalPath` is provided - assume that operation possibly may be a staging operation.
    // To know for sure, fetch metadata and check a `isStagingOperation` flag. If it happens that it wasn't
    // a staging operation - not a big deal, we just fetched metadata earlier, but operation is still usable
    // and user can get data from it.
    // If `stagingAllowedLocalPath` is not provided - don't do anything to the operation. In a case of regular
    // operation, everything will work as usual. In a case of staging operation, it will be processed like any
    // other query - it will be possible to get data from it as usual, or use other operation methods.
    if (options.stagingAllowedLocalPath !== undefined) {
      const metadata = await operation.getMetadata();
      if (metadata.isStagingOperation) {
        const allowedLocalPath = Array.isArray(options.stagingAllowedLocalPath)
          ? options.stagingAllowedLocalPath
          : [options.stagingAllowedLocalPath];
        return this.handleStagingOperation(operation, allowedLocalPath);
      }
    }
    return operation;
  }

  private async handleStagingOperation(operation: IOperation, allowedLocalPath: Array<string>): Promise<IOperation> {
    type StagingResponse = {
      presignedUrl: string;
      localFile?: string;
      headers: HeadersInit;
      operation: string;
    };
    const rows = await operation.fetchAll();
    if (rows.length !== 1) {
      throw new StagingError('Staging operation: expected only one row in result');
    }
    const row = rows[0] as StagingResponse;

    // For REMOVE operation local file is not available, so no need to validate it
    if (row.localFile !== undefined) {
      let allowOperation = false;

      for (const filepath of allowedLocalPath) {
        const relativePath = path.relative(filepath, row.localFile);

        if (!relativePath.startsWith('..') && !path.isAbsolute(relativePath)) {
          allowOperation = true;
        }
      }

      if (!allowOperation) {
        throw new StagingError('Staging path not a subset of allowed local paths.');
      }
    }

    const { localFile, presignedUrl, headers } = row;

    switch (row.operation) {
      case 'GET':
        await this.handleStagingGet(localFile, presignedUrl, headers);
        return operation;
      case 'PUT':
        await this.handleStagingPut(localFile, presignedUrl, headers);
        return operation;
      case 'REMOVE':
        await this.handleStagingRemove(presignedUrl, headers);
        return operation;
      default:
        throw new StagingError(`Staging query operation is not supported: ${row.operation}`);
    }
  }

  private async handleStagingGet(
    localFile: string | undefined,
    presignedUrl: string,
    headers: HeadersInit,
  ): Promise<void> {
    if (localFile === undefined) {
      throw new StagingError('Local file path not provided');
    }

    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();

    const response = await fetch(presignedUrl, { method: 'GET', headers, agent });
    if (!response.ok) {
      throw new StagingError(`HTTP error ${response.status} ${response.statusText}`);
    }
    const buffer = await response.arrayBuffer();
    fs.writeFileSync(localFile, Buffer.from(buffer));
  }

  private async handleStagingRemove(presignedUrl: string, headers: HeadersInit): Promise<void> {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();

    const response = await fetch(presignedUrl, { method: 'DELETE', headers, agent });
    if (!response.ok) {
      throw new StagingError(`HTTP error ${response.status} ${response.statusText}`);
    }
  }

  private async handleStagingPut(
    localFile: string | undefined,
    presignedUrl: string,
    headers: HeadersInit,
  ): Promise<void> {
    if (localFile === undefined) {
      throw new StagingError('Local file path not provided');
    }

    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();

    const data = fs.readFileSync(localFile);
    const response = await fetch(presignedUrl, { method: 'PUT', headers, agent, body: data });
    if (!response.ok) {
      throw new StagingError(`HTTP error ${response.status} ${response.statusText}`);
    }
  }

  /**
   * Information about supported data types
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getTypeInfo(request: TypeInfoRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getTypeInfo({
      sessionHandle: this.sessionHandle,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Get list of catalogs
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getCatalogs(request: CatalogsRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getCatalogs({
      sessionHandle: this.sessionHandle,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Get list of schemas
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getSchemas(request: SchemasRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getSchemas({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Get list of tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getTables(request: TablesRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getTables({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      tableTypes: request.tableTypes,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Get list of supported table types
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getTableTypes(request: TableTypesRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getTableTypes({
      sessionHandle: this.sessionHandle,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Get full information about columns of the table
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getColumns(request: ColumnsRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getColumns({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      columnName: request.columnName,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Get information about function
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getFunctions(request: FunctionsRequest): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getFunctions({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      functionName: request.functionName,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  public async getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getPrimaryKeys({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Request information about foreign keys between two tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getCrossReference(request: CrossReferenceRequest): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const operationPromise = driver.getCrossReference({
      sessionHandle: this.sessionHandle,
      parentCatalogName: request.parentCatalogName,
      parentSchemaName: request.parentSchemaName,
      parentTableName: request.parentTableName,
      foreignCatalogName: request.foreignCatalogName,
      foreignSchemaName: request.foreignSchemaName,
      foreignTableName: request.foreignTableName,
      runAsync: true,
      ...getDirectResultsOptions(request.maxRows),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  /**
   * Closes the session
   * @public
   * @returns Operation status
   */
  public async close(): Promise<Status> {
    if (!this.isOpen) {
      return Status.success();
    }

    // Close owned operations one by one, removing successfully closed ones from the list
    await this.operations.closeAll();

    const driver = await this.context.getDriver();
    const response = await driver.closeSession({
      sessionHandle: this.sessionHandle,
    });
    // check status for being successful
    Status.assert(response.status);

    // notify owner connection
    this.onClose?.();
    this.isOpen = false;

    this.context.getLogger().log(LogLevel.debug, `Session closed with id: ${this.getId()}`);
    return new Status(response.status);
  }

  private createOperation(response: OperationResponseShape): DBSQLOperation {
    Status.assert(response.status);
    const handle = definedOrError(response.operationHandle);
    const operation = new DBSQLOperation({
      handle,
      directResults: response.directResults,
      context: this.context,
    });

    this.operations.add(operation);

    return operation;
  }

  private async failIfClosed(): Promise<void> {
    if (!this.isOpen) {
      throw new HiveDriverError('The session was closed or has expired');
    }
  }

  private async handleResponse<T>(requestPromise: Promise<T>): Promise<T> {
    // Currently, after being closed sessions remains usable - server will not
    // error out when trying to run operations on closed session. So it's
    // basically useless to process any errors here
    const result = await requestPromise;
    await this.failIfClosed();
    return result;
  }
}
