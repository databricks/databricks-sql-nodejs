import Int64 from 'node-int64';
import IBackend from '../contracts/IBackend';
import ISessionBackend from '../contracts/ISessionBackend';
import IClientContext from '../contracts/IClientContext';
import { ConnectionOptions, OpenSessionRequest } from '../contracts/IDBSQLClient';
import { TProtocolVersion } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import { definedOrError, serializeQueryTags } from '../utils';
import ThriftSessionBackend from './ThriftSessionBackend';
import StatusError from '../errors/StatusError';
import reydenCache from '../ReydenWarehouseCache';
import KernelBackend from '../kernel/KernelBackend';
import { LogLevel } from '../contracts/IDBSQLLogger';

function getInitialNamespaceOptions(catalogName?: string, schemaName?: string) {
  if (!catalogName && !schemaName) {
    return {};
  }

  return {
    initialNamespace: {
      catalogName,
      schemaName,
    },
  };
}

interface ThriftBackendOptions {
  context: IClientContext;
  onConnectionEvent: (event: 'error' | 'reconnecting' | 'close' | 'timeout', payload?: unknown) => void;
}

export default class ThriftBackend implements IBackend {
  private readonly context: IClientContext;

  private readonly onConnectionEvent: ThriftBackendOptions['onConnectionEvent'];

  private connectionOptions?: ConnectionOptions;

  // The memoized connect for a single KernelBackend, reused across every Reyden (KP001)
  // fallback session on this connection. connect() installs a process-global log-bridge
  // listener, so the backend is created once (connectionOptions are fixed after connect)
  // and released in close() — rather than constructing one per openSession and leaking a
  // listener each time. This promise is the single source of truth for the fallback
  // backend; its resolved value is the backend.
  private fallbackKernelBackendConnect?: Promise<KernelBackend>;

  constructor({ context, onConnectionEvent }: ThriftBackendOptions) {
    this.context = context;
    this.onConnectionEvent = onConnectionEvent;
  }

  /**
   * Extracts warehouse/endpoint ID from the HTTP path.
   * Matches patterns like `/sql/1.0/warehouses/<id>` or `/sql/1.0/endpoints/<id>`.
   * Returns undefined if no ID can be extracted.
   */
  private static extractWarehouseId(httpPath: string | undefined): string | undefined {
    if (!httpPath) {
      return undefined;
    }

    // Stop at query string
    const pathOnly = httpPath.split('?')[0];

    // Match `/warehouses/<id>` or `/endpoints/<id>`
    // Stop at `/` or end of string
    const match = pathOnly.match(/\/(warehouses|endpoints)\/([^/]+)/);
    return match ? match[2] : undefined;
  }

  public async connect(options: ConnectionOptions): Promise<void> {
    // Store connection options for warehouse ID extraction in openSession
    this.connectionOptions = options;

    // The connection provider is owned by DBSQLClient (it implements IClientContext).
    // We only need to wire the EventEmitter listeners through this backend.
    const connectionProvider = await this.context.getConnectionProvider();
    const thriftConnection = await connectionProvider.getThriftConnection();

    thriftConnection.on('error', (error: Error) => {
      this.onConnectionEvent('error', error);
    });

    thriftConnection.on('reconnecting', (params: { delay: number; attempt: number }) => {
      this.onConnectionEvent('reconnecting', params);
    });

    thriftConnection.on('close', () => {
      this.onConnectionEvent('close');
    });

    thriftConnection.on('timeout', () => {
      this.onConnectionEvent('timeout');
    });
  }

  public async openSession(request: OpenSessionRequest): Promise<ISessionBackend> {
    const logger = this.context.getLogger();

    // Extract warehouse ID for cache lookups
    const warehouseId = ThriftBackend.extractWarehouseId(this.connectionOptions?.path);
    const host = this.connectionOptions?.host;

    // Check if this warehouse is known to be Reyden (requires SEA backend)
    if (host && warehouseId && reydenCache.isKnownReyden(host, warehouseId)) {
      logger.log(LogLevel.debug, `Reyden: warehouse ${warehouseId} is known to require SEA fallback; skipping Thrift`);
      return this.openSessionWithKernelBackend(request);
    }

    // Try Thrift first (default path).
    try {
      return await this.openSessionWithThrift(request);
    } catch (error) {
      // Only a Reyden KP001 rejection triggers fallback. Every other error
      // propagates unchanged — note StatusError is NOT an Error subclass
      // (it only `implements Error`), so it must be re-thrown as-is rather
      // than normalized, or its sqlState/message would be lost.
      if (error instanceof StatusError && error.sqlState === 'KP001') {
        logger.log(LogLevel.debug, `Reyden: detected KP001 on warehouse ${warehouseId}; falling back to SEA backend`);

        // Mark this warehouse as Reyden for future connections.
        if (host && warehouseId) {
          reydenCache.markReyden(host, warehouseId);
        }

        // Fall back to the kernel (SEA) backend exactly once. If it also fails,
        // surface the kernel error but keep the original Thrift rejection as its
        // cause for diagnosis.
        try {
          return await this.openSessionWithKernelBackend(request);
        } catch (kernelError) {
          // Preserve the Thrift KP001 as the kernel error's cause, but don't clobber a cause
          // the kernel error may already carry.
          if (
            kernelError &&
            typeof kernelError === 'object' &&
            (kernelError as { cause?: unknown }).cause === undefined
          ) {
            (kernelError as { cause?: unknown }).cause = error;
          }
          logger.log(LogLevel.error, 'Reyden: both Thrift (KP001) and SEA fallback failed');
          throw kernelError;
        }
      }

      // Not a Reyden rejection — surface the original error unchanged.
      throw error;
    }
  }

  /**
   * Opens a session using the Thrift backend.
   */
  private async openSessionWithThrift(request: OpenSessionRequest): Promise<ISessionBackend> {
    const driver = await this.context.getDriver();
    const config = this.context.getConfig();

    const configuration = request.configuration ? { ...request.configuration } : {};

    if (config.enableMetricViewMetadata) {
      configuration['spark.sql.thriftserver.metadata.metricview.enabled'] = 'true';
    }

    if (request.queryTags !== undefined) {
      const serialized = serializeQueryTags(request.queryTags);
      if (serialized) {
        configuration.QUERY_TAGS = serialized;
      } else {
        delete configuration.QUERY_TAGS;
      }
    }

    const response = await driver.openSession({
      client_protocol_i64: new Int64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8),
      ...getInitialNamespaceOptions(request.initialCatalog, request.initialSchema),
      configuration,
      canUseMultipleCatalogs: true,
    });

    Status.assert(response.status);
    return new ThriftSessionBackend({
      handle: definedOrError(response.sessionHandle),
      context: this.context,
      serverProtocolVersion: response.serverProtocolVersion,
    });
  }

  /**
   * Opens a session using the KernelBackend (SEA).
   * Called as a fallback when Thrift returns KP001 (Reyden rejection).
   */
  private async openSessionWithKernelBackend(request: OpenSessionRequest): Promise<ISessionBackend> {
    if (!this.connectionOptions) {
      throw new Error('KernelBackend fallback: connection options not available');
    }

    const logger = this.context.getLogger();
    logger.log(LogLevel.debug, 'Reyden: opening session via KernelBackend (SEA)');

    const kernelBackend = await this.getFallbackKernelBackend(this.connectionOptions);
    return kernelBackend.openSession(request);
  }

  // Lazily creates and connects the single fallback KernelBackend, reused across every
  // fallback session so repeated opens don't accumulate backends / log-bridge listeners.
  // On a connect failure the memoized attempt is cleared so a later open can retry.
  private getFallbackKernelBackend(connectionOptions: ConnectionOptions): Promise<KernelBackend> {
    if (!this.fallbackKernelBackendConnect) {
      this.fallbackKernelBackendConnect = (async () => {
        const kernelBackend = this.createKernelBackend();
        await kernelBackend.connect(connectionOptions);
        return kernelBackend;
      })().catch((error) => {
        this.fallbackKernelBackendConnect = undefined;
        throw error;
      });
    }
    return this.fallbackKernelBackendConnect;
  }

  // Seam so tests can inject a fake KernelBackend without the native binding.
  protected createKernelBackend(): KernelBackend {
    return new KernelBackend({ context: this.context });
  }

  public async close(): Promise<void> {
    // Release the process-global log-bridge listener held by the Reyden-fallback KernelBackend.
    // DBSQLClient owns the rest of the connection lifecycle and clears its own state
    // (connectionProvider, authProvider, thrift client) after this returns.
    //
    // Await the in-flight connect rather than a resolved-backend field: the connect
    // installs the listener only once it resolves, so a close() racing an unresolved
    // fallback connect must still wait for it and release the backend it produces.
    // Clear the field first so the state is consistent even if the awaited close() throws.
    const pendingConnect = this.fallbackKernelBackendConnect;
    this.fallbackKernelBackendConnect = undefined;
    if (pendingConnect) {
      const kernelBackend = await pendingConnect.catch(() => undefined);
      await kernelBackend?.close();
    }
  }
}
