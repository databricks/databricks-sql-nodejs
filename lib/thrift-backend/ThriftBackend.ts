import Int64 from 'node-int64';
import IBackend from '../contracts/IBackend';
import ISessionBackend from '../contracts/ISessionBackend';
import IClientContext from '../contracts/IClientContext';
import { OpenSessionRequest } from '../contracts/IDBSQLClient';
import { TProtocolVersion } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import { definedOrError, serializeQueryTags } from '../utils';
import ThriftSessionBackend from './ThriftSessionBackend';

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

  constructor({ context, onConnectionEvent }: ThriftBackendOptions) {
    this.context = context;
    this.onConnectionEvent = onConnectionEvent;
  }

  public async connect(): Promise<void> {
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

  public async close(): Promise<void> {
    // DBSQLClient owns the connection lifecycle and clears its own state
    // (connectionProvider, authProvider, thrift client) after this returns.
  }
}
