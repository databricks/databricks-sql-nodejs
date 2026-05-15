import { stringify, NIL } from 'uuid';
import Int64 from 'node-int64';
import {
  TSessionHandle,
  TStatus,
  TOperationHandle,
  TSparkDirectResults,
  TSparkArrowTypes,
  TSparkParameter,
  TProtocolVersion,
  TExecuteStatementReq,
} from '../../thrift/TCLIService_types';
import ISessionBackend from '../contracts/ISessionBackend';
import IOperationBackend from '../contracts/IOperationBackend';
import IClientContext, { ClientConfig } from '../contracts/IClientContext';
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
} from '../contracts/IDBSQLSession';
import Status from '../dto/Status';
import InfoValue from '../dto/InfoValue';
import { definedOrError, LZ4, ProtocolVersion, serializeQueryTags } from '../utils';
import ParameterError from '../errors/ParameterError';
import { DBSQLParameter, DBSQLParameterValue } from '../DBSQLParameter';
import ThriftOperationBackend from './ThriftOperationBackend';

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

interface ThriftSessionBackendOptions {
  handle: TSessionHandle;
  context: IClientContext;
  serverProtocolVersion?: TProtocolVersion;
}

export default class ThriftSessionBackend implements ISessionBackend {
  private readonly context: IClientContext;

  private readonly sessionHandle: TSessionHandle;

  private readonly serverProtocolVersion?: TProtocolVersion;

  constructor({ handle, context, serverProtocolVersion }: ThriftSessionBackendOptions) {
    this.sessionHandle = handle;
    this.context = context;
    this.serverProtocolVersion = serverProtocolVersion;
  }

  private getRunAsyncForMetadataOperations(): boolean | undefined {
    return ProtocolVersion.supportsAsyncMetadataOperations(this.serverProtocolVersion) ? true : undefined;
  }

  public get id(): string {
    const sessionId = this.sessionHandle?.sessionId?.guid;
    return sessionId ? stringify(sessionId) : NIL;
  }

  public async getInfo(infoType: number): Promise<InfoValue> {
    const driver = await this.context.getDriver();
    const response = await driver.getInfo({
      sessionHandle: this.sessionHandle,
      infoType,
    });
    Status.assert(response.status);
    return new InfoValue(response.infoValue);
  }

  public async executeStatement(statement: string, options: ExecuteStatementOptions): Promise<IOperationBackend> {
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

    const serializedQueryTags = serializeQueryTags(options.queryTags);
    if (serializedQueryTags !== undefined) {
      request.confOverlay = { ...request.confOverlay, query_tags: serializedQueryTags };
    }

    if (ProtocolVersion.supportsCloudFetch(this.serverProtocolVersion)) {
      request.canDownloadResult = options.useCloudFetch ?? clientConfig.useCloudFetch;
    }

    if (ProtocolVersion.supportsArrowCompression(this.serverProtocolVersion) && request.canDownloadResult !== true) {
      request.canDecompressLZ4Result = (options.useLZ4Compression ?? clientConfig.useLZ4Compression) && Boolean(LZ4());
    }

    const response = await driver.executeStatement(request);
    return this.createOperationBackend(response);
  }

  public async getTypeInfo(request: TypeInfoRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getTypeInfo({
      sessionHandle: this.sessionHandle,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getCatalogs(request: CatalogsRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getCatalogs({
      sessionHandle: this.sessionHandle,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getSchemas(request: SchemasRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getSchemas({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getTables(request: TablesRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getTables({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      tableTypes: request.tableTypes,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getTableTypes(request: TableTypesRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getTableTypes({
      sessionHandle: this.sessionHandle,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getColumns(request: ColumnsRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getColumns({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      columnName: request.columnName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getFunctions(request: FunctionsRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getFunctions({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      functionName: request.functionName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getPrimaryKeys({
      sessionHandle: this.sessionHandle,
      catalogName: request.catalogName,
      schemaName: request.schemaName,
      tableName: request.tableName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async getCrossReference(request: CrossReferenceRequest): Promise<IOperationBackend> {
    const driver = await this.context.getDriver();
    const response = await driver.getCrossReference({
      sessionHandle: this.sessionHandle,
      parentCatalogName: request.parentCatalogName,
      parentSchemaName: request.parentSchemaName,
      parentTableName: request.parentTableName,
      foreignCatalogName: request.foreignCatalogName,
      foreignSchemaName: request.foreignSchemaName,
      foreignTableName: request.foreignTableName,
      runAsync: this.getRunAsyncForMetadataOperations(),
      ...getDirectResultsOptions(request.maxRows, this.context.getConfig()),
    });
    return this.createOperationBackend(response);
  }

  public async close(): Promise<Status> {
    const driver = await this.context.getDriver();
    const response = await driver.closeSession({
      sessionHandle: this.sessionHandle,
    });
    Status.assert(response.status);
    return new Status(response.status);
  }

  private createOperationBackend(response: OperationResponseShape): IOperationBackend {
    Status.assert(response.status);
    const handle = definedOrError(response.operationHandle);
    return new ThriftOperationBackend({
      handle,
      directResults: response.directResults,
      context: this.context,
    });
  }
}
