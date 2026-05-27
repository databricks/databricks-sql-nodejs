import * as fs from 'fs';
import * as path from 'path';
import stream from 'node:stream';
import util from 'node:util';
import fetch, { HeadersInit } from 'node-fetch';
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
import { numberToInt64 as numberToInt64Impl } from './thrift-backend/ThriftSessionBackend';

// Explicitly promisify a callback-style `pipeline` because `node:stream/promises` is not available in Node 14
const pipeline = util.promisify(stream.pipeline);

/**
 * Convert a JS number to a Thrift-wire `node-int64`.
 *
 * @deprecated Thrift-only utility re-exported for back-compat with existing
 * external consumers. Backends other than Thrift do not use `node-int64`;
 * new code should not import this from `DBSQLSession`. It will be removed
 * when the public API stops exposing Thrift wire types.
 */
export const numberToInt64 = numberToInt64Impl;

interface DBSQLSessionConstructorOptions {
  backend: ISessionBackend;
  context: IClientContext;
}

export default class DBSQLSession implements IDBSQLSession {
  private readonly context: IClientContext;

  private readonly backend: ISessionBackend;

  private isOpen = true;

  public onClose?: () => void;

  private operations = new CloseableCollection<DBSQLOperation>();

  constructor(options: DBSQLSessionConstructorOptions) {
    this.context = options.context;
    this.backend = options.backend;
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
    return this.runBackend(() => this.backend.getInfo(infoType));
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
    const opBackend = await this.runBackend(() => this.backend.executeStatement(statement, options));
    const operation = this.wrapOperation(opBackend);

    // If `stagingAllowedLocalPath` is provided - assume that operation possibly may be a staging operation.
    // To know for sure, fetch metadata and check a `isStagingOperation` flag. If it happens that it wasn't
    // a staging operation - not a big deal, we just fetched metadata earlier, but operation is still usable
    // and user can get data from it.
    // If `stagingAllowedLocalPath` is not provided - don't do anything to the operation. In a case of regular
    // operation, everything will work as usual. In a case of staging operation, it will be processed like any
    // other query - it will be possible to get data from it as usual, or use other operation methods.
    if (options.stagingAllowedLocalPath !== undefined) {
      const metadata = await operation.getResultMetadata();
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
    return this.wrapOperation(await this.runBackend(() => this.backend.getTypeInfo(request)));
  }

  /**
   * Get list of catalogs
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getCatalogs(request: CatalogsRequest = {}): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getCatalogs(request)));
  }

  /**
   * Get list of schemas
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getSchemas(request: SchemasRequest = {}): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getSchemas(request)));
  }

  /**
   * Get list of tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getTables(request: TablesRequest = {}): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getTables(request)));
  }

  /**
   * Get list of supported table types
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getTableTypes(request: TableTypesRequest = {}): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getTableTypes(request)));
  }

  /**
   * Get full information about columns of the table
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getColumns(request: ColumnsRequest = {}): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getColumns(request)));
  }

  /**
   * Get information about function
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getFunctions(request: FunctionsRequest): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getFunctions(request)));
  }

  public async getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getPrimaryKeys(request)));
  }

  /**
   * Request information about foreign keys between two tables
   * @public
   * @param request
   * @returns DBSQLOperation
   */
  public async getCrossReference(request: CrossReferenceRequest): Promise<IOperation> {
    return this.wrapOperation(await this.runBackend(() => this.backend.getCrossReference(request)));
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

  /**
   * Bracket a backend call with `failIfClosed()` on both sides. The pre-call
   * check rejects work against an already-closed session; the post-call check
   * rejects results that came back after a concurrent close (server-side
   * close doesn't error out the in-flight RPC). Centralizing the pattern
   * keeps the 10+ delegation methods readable and makes the contract
   * impossible to forget.
   */
  private async runBackend<T>(fn: () => Promise<T>): Promise<T> {
    await this.failIfClosed();
    const result = await fn();
    await this.failIfClosed();
    return result;
  }

  private async failIfClosed(): Promise<void> {
    if (!this.isOpen) {
      throw new HiveDriverError('The session was closed or has expired');
    }
  }
}
