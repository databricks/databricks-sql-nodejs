import * as fs from 'fs';
import * as path from 'path';
import stream from 'node:stream';
import util from 'node:util';
import fetch, { HeadersInit } from 'node-fetch';
import { TSessionHandle, TProtocolVersion } from '../thrift/TCLIService_types';
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
import CloseableCollection from './utils/CloseableCollection';
import { LogLevel } from './contracts/IDBSQLLogger';
import HiveDriverError from './errors/HiveDriverError';
import StagingError from './errors/StagingError';
import IClientContext from './contracts/IClientContext';
import ISessionBackend from './contracts/ISessionBackend';
import IOperationBackend from './contracts/IOperationBackend';
import ThriftSessionBackend from './thrift-backend/ThriftSessionBackend';

// Explicitly promisify a callback-style `pipeline` because `node:stream/promises` is not available in Node 14
const pipeline = util.promisify(stream.pipeline);

// Re-export for back-compat with existing imports.
export { numberToInt64 } from './thrift-backend/ThriftSessionBackend';

type DBSQLSessionConstructorOptions =
  | {
      handle: TSessionHandle;
      context: IClientContext;
      serverProtocolVersion?: TProtocolVersion;
    }
  | {
      backend: ISessionBackend;
      context: IClientContext;
    };

export default class DBSQLSession implements IDBSQLSession {
  private readonly context: IClientContext;

  private readonly backend: ISessionBackend;

  private isOpen = true;

  public onClose?: () => void;

  private operations = new CloseableCollection<DBSQLOperation>();

  constructor(options: DBSQLSessionConstructorOptions) {
    this.context = options.context;
    this.backend =
      'backend' in options
        ? options.backend
        : new ThriftSessionBackend({
            handle: options.handle,
            context: options.context,
            serverProtocolVersion: options.serverProtocolVersion,
          });
    this.context.getLogger().log(LogLevel.debug, `Session created with id: ${this.id}`);
  }

  public get id() {
    return this.backend.id;
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
    const result = await this.backend.getInfo(infoType);
    await this.failIfClosed();
    return result;
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
    const opBackend = await this.backend.executeStatement(statement, options);
    await this.failIfClosed();
    const operation = this.wrapOperation(opBackend);

    // Staging detection: only run when stagingAllowedLocalPath is provided.
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
    return pipeline(response.body, fileStream);
  }

  private async handleStagingRemove(presignedUrl: string, headers: HeadersInit): Promise<void> {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();

    const response = await fetch(presignedUrl, { method: 'DELETE', headers, agent });
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
    const opBackend = await this.backend.getTypeInfo(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  /**
   * Get list of catalogs
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getCatalogs(request: CatalogsRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getCatalogs(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  /**
   * Get list of schemas
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getSchemas(request: SchemasRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getSchemas(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  /**
   * Get list of tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getTables(request: TablesRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getTables(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  /**
   * Get list of supported table types
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getTableTypes(request: TableTypesRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getTableTypes(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  /**
   * Get full information about columns of the table
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getColumns(request: ColumnsRequest = {}): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getColumns(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  /**
   * Get information about function
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getFunctions(request: FunctionsRequest): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getFunctions(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  public async getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getPrimaryKeys(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
  }

  /**
   * Request information about foreign keys between two tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getCrossReference(request: CrossReferenceRequest): Promise<IOperation> {
    await this.failIfClosed();
    const opBackend = await this.backend.getCrossReference(request);
    await this.failIfClosed();
    return this.wrapOperation(opBackend);
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

    await this.operations.closeAll();

    const status = await this.backend.close();

    this.onClose?.();
    this.isOpen = false;

    this.context.getLogger().log(LogLevel.debug, `Session closed with id: ${this.id}`);
    return status;
  }

  private wrapOperation(backend: IOperationBackend): DBSQLOperation {
    const operation = new DBSQLOperation({ backend, context: this.context });
    this.operations.add(operation);
    return operation;
  }

  private async failIfClosed(): Promise<void> {
    if (!this.isOpen) {
      throw new HiveDriverError('The session was closed or has expired');
    }
  }
}
