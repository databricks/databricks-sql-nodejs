import * as fs from 'fs';
import * as path from 'path';
import stream from 'node:stream';
import util from 'node:util';
import { stringify, NIL } from 'uuid';
import Int64 from 'node-int64';
import fetch, { HeadersInit } from 'node-fetch';
import {
  TSessionHandle,
  TStatus,
  TOperationHandle,
  TSparkDirectResults,
  TSparkArrowTypes,
  TSparkParameter,
  TProtocolVersion,
  TExecuteStatementReq,
} from '../thrift/TCLIService_types';
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
import { definedOrError, LZ4, ProtocolVersion } from './utils';
import CloseableCollection from './utils/CloseableCollection';
import { LogLevel } from './contracts/IDBSQLLogger';
import HiveDriverError from './errors/HiveDriverError';
import StagingError from './errors/StagingError';
import { DBSQLParameter, DBSQLParameterValue } from './DBSQLParameter';
import ParameterError from './errors/ParameterError';
import IClientContext, { ClientConfig } from './contracts/IClientContext';

// Explicitly promisify a callback-style `pipeline` because `node:stream/promises` is not available in Node 14
const pipeline = util.promisify(stream.pipeline);

interface OperationResponseShape {
  status: TStatus;
  operationHandle?: TOperationHandle;
  directResults?: TSparkDirectResults;
}

export function numberToInt64(value: number | bigint | Int64): Int64 {
  if (value instanceof Int64) {
    return value;
  }

  if (typeof value === 'bigint') {
    const buffer = new ArrayBuffer(BigInt64Array.BYTES_PER_ELEMENT);
    const view = new DataView(buffer);
    view.setBigInt64(0, value, false); // `false` to use big-endian order
    return new Int64(Buffer.from(buffer));
  }

  return new Int64(value);
}

function getDirectResultsOptions(maxRows: number | bigint | Int64 | null | undefined, config: ClientConfig) {
  if (maxRows === null) {
    return {};
  }

  return {
    getDirectResults: {
      maxRows: numberToInt64(maxRows ?? config.directResultsDefaultMaxRows),
    },
  };
}

function getArrowOptions(
  config: ClientConfig,
  serverProtocolVersion: TProtocolVersion | undefined | null,
): {
  canReadArrowResult: boolean;
  useArrowNativeTypes?: TSparkArrowTypes;
} {
  const { arrowEnabled = true, useArrowNativeTypes = true } = config;

  if (!arrowEnabled || !ProtocolVersion.supportsArrowMetadata(serverProtocolVersion)) {
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
  serverProtocolVersion?: TProtocolVersion;
}

export default class DBSQLSession implements IDBSQLSession {
  private readonly context: IClientContext;

  private readonly sessionHandle: TSessionHandle;

  private isOpen = true;

  private openTime: number;

  private serverProtocolVersion?: TProtocolVersion;

  public onClose?: () => void;

  private operations = new CloseableCollection<DBSQLOperation>();

  /**
   * Helper method to determine if runAsync should be set for metadata operations
   * @private
   * @returns true if supported by protocol version, undefined otherwise
   */
  private getRunAsyncForMetadataOperations(): boolean | undefined {
    return ProtocolVersion.supportsAsyncMetadataOperations(this.serverProtocolVersion) ? true : undefined;
  }

  constructor({ handle, context, serverProtocolVersion }: DBSQLSessionConstructorOptions) {
    this.sessionHandle = handle;
    this.context = context;
    this.openTime = Date.now();
    // Get the server protocol version from the provided parameter (from TOpenSessionResp)
    this.serverProtocolVersion = serverProtocolVersion;
    this.context.getLogger().log(LogLevel.debug, `Session created with id: ${this.id}`);
    this.context.getLogger().log(LogLevel.debug, `Server protocol version: ${this.serverProtocolVersion}`);
  }

  public get id() {
    const sessionId = this.sessionHandle?.sessionId?.guid;
    return sessionId ? stringify(sessionId) : NIL;
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

    const request = new TExecuteStatementReq({
      sessionHandle: this.sessionHandle,
      statement,
      queryTimeout: options.queryTimeout ? numberToInt64(options.queryTimeout) : undefined,
      runAsync: true,
      ...getDirectResultsOptions(options.maxRows, clientConfig),
      ...getArrowOptions(clientConfig, this.serverProtocolVersion),
    });

    if (ProtocolVersion.supportsParameterizedQueries(this.serverProtocolVersion)) {
      request.parameters = getQueryParameters(options.namedParameters, options.ordinalParameters);
    }

    if (ProtocolVersion.supportsCloudFetch(this.serverProtocolVersion)) {
      request.canDownloadResult = options.useCloudFetch ?? clientConfig.useCloudFetch;
    }

    if (ProtocolVersion.supportsArrowCompression(this.serverProtocolVersion) && request.canDownloadResult !== true) {
      request.canDecompressLZ4Result = (options.useLZ4Compression ?? clientConfig.useLZ4Compression) && Boolean(LZ4());
    }

    const operationPromise = driver.executeStatement(request);
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

    const fileStream = fs.createWriteStream(localFile);
    // `pipeline` will do all the dirty job for us, including error handling and closing all the streams properly
    return pipeline(response.body, fileStream);
  }

  private async handleStagingRemove(presignedUrl: string, headers: HeadersInit): Promise<void> {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();

    const response = await fetch(presignedUrl, { method: 'DELETE', headers, agent });
    // Looks that AWS and Azure have a different behavior of HTTP `DELETE` for non-existing files.
    // AWS assumes that - since file already doesn't exist - the goal is achieved, and returns HTTP 200.
    // Azure, on the other hand, is somewhat stricter and check if file exists before deleting it. And if
    // file doesn't exist - Azure returns HTTP 404.
    //
    // For us, it's totally okay if file didn't exist before removing. So when we get an HTTP 404 -
    // just ignore it and report success. This way we can have a uniform library behavior for all clouds
    if (!response.ok && response.status !== 404) {
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

    const fileStream = fs.createReadStream(localFile);
    const fileInfo = fs.statSync(localFile, { bigint: true });

    const response = await fetch(presignedUrl, {
      method: 'PUT',
      headers: {
        ...headers,
        // This header is required by server
        'Content-Length': fileInfo.size.toString(),
      },
      agent,
      body: fileStream,
    });
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getTypeInfo({
      sessionHandle: this.sessionHandle,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getCatalogs({
      sessionHandle: this.sessionHandle,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getSchemas({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getTables({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      tableTypes: request.tableTypes,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getTableTypes({
      sessionHandle: this.sessionHandle,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getColumns({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      columnName: request.columnName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getFunctions({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      functionName: request.functionName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
    });
    const response = await this.handleResponse(operationPromise);
    return this.createOperation(response);
  }

  public async getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation> {
    await this.failIfClosed();
    const driver = await this.context.getDriver();
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getPrimaryKeys({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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
    const clientConfig = this.context.getConfig();

    const operationPromise = driver.getCrossReference({
      sessionHandle: this.sessionHandle,
      parentCatalogName: request.parentCatalogName,
      parentSchemaName: request.parentSchemaName,
      parentTableName: request.parentTableName,
      foreignCatalogName: request.foreignCatalogName,
      foreignSchemaName: request.foreignSchemaName,
      foreignTableName: request.foreignTableName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, clientConfig),
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

    // Emit connection close telemetry
    const closeLatency = Date.now() - this.openTime;
    const { telemetryEmitter } = this.context as any;
    if (telemetryEmitter) {
      telemetryEmitter.emitConnectionClose({
        sessionId: this.id,
        latencyMs: closeLatency,
      });
    }

    this.context.getLogger().log(LogLevel.debug, `Session closed with id: ${this.id}`);
    return new Status(response.status);
  }

  private createOperation(response: OperationResponseShape): DBSQLOperation {
    Status.assert(response.status);
    const handle = definedOrError(response.operationHandle);
    const operation = new DBSQLOperation({
      handle,
      directResults: response.directResults,
      context: this.context,
      sessionId: this.id,
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
