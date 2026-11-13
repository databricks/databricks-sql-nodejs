import http from 'http';
import { AddressInfo } from 'net';
import { expect } from 'chai';
import sinon from 'sinon';
import { TBinaryProtocol, TBufferedTransport } from 'thrift';
import DBSQLClient from '../../../../lib/DBSQLClient';
import HttpConnection from '../../../../lib/connection/connections/HttpConnection';
import FeatureFlagCache from '../../../../lib/FeatureFlagCache';
import HiveDriverError from '../../../../lib/errors/HiveDriverError';
import { ConnectionOptions } from '../../../../lib/contracts/IDBSQLClient';
import { LogLevel } from '../../../../lib/contracts/IDBSQLLogger';
import * as TCLIService from '../../../../thrift/TCLIService';
import {
  TCloseSessionResp,
  TExecuteStatementResp,
  TOpenSessionResp,
  TProtocolVersion,
  TStatusCode,
} from '../../../../thrift/TCLIService_types';
import ClientContextStub from '../../.stubs/ClientContextStub';

interface RecordedRequest {
  method: string;
  headers: http.IncomingHttpHeaders;
}

// Minimal Thrift-over-HTTP server: records each request's Thrift method name and
// HTTP headers, opens sessions successfully and fails every statement.
async function startFakeThriftServer() {
  const requests: Array<RecordedRequest> = [];
  const processor = new TCLIService.Processor({
    OpenSession: (_req: unknown) =>
      new TOpenSessionResp({
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
        sessionHandle: {
          sessionId: { guid: Buffer.from('0123456789ab4def8123456789abcdef', 'hex'), secret: Buffer.alloc(16, 2) },
        },
      }),
    ExecuteStatement: (_req: unknown) =>
      new TExecuteStatementResp({ status: { statusCode: TStatusCode.ERROR_STATUS, errorMessage: 'fake server' } }),
    CloseSession: (_req: unknown) => new TCloseSessionResp({ status: { statusCode: TStatusCode.SUCCESS_STATUS } }),
  });

  const server = http.createServer((req, res) => {
    const chunks: Array<Buffer> = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      TBufferedTransport.receiver((transport) => {
        const input = new TBinaryProtocol(transport);
        requests.push({ method: input.readMessageBegin().fname, headers: req.headers });
        transport.rollbackPosition();
        const output = new TBinaryProtocol(
          new TBufferedTransport(undefined, (message?: Buffer) => {
            res.writeHead(200, { 'Content-Type': 'application/x-thrift' });
            res.end(message);
          }),
        );
        processor.process(input, output);
      }, 0)(Buffer.concat(chunks));
    });
  });

  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });

  return {
    port: (server.address() as AddressInfo).port,
    requests,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  };
}

describe('ConnectionOptions.customHeaders on the Thrift transport', () => {
  let server: Awaited<ReturnType<typeof startFakeThriftServer>>;

  beforeEach(async () => {
    sinon.stub(FeatureFlagCache.prototype as any, 'fetchFeatureFlags').resolves(new Map());
    server = await startFakeThriftServer();
  });

  afterEach(async () => {
    sinon.restore();
    await server.close();
  });

  // `DBSQLClient` always connects over https; swap in plain http so the requests
  // reach the local server, keeping every other connection option intact.
  function createClient(): DBSQLClient {
    const client = new DBSQLClient();
    sinon
      .stub(client as any, 'createConnectionProvider')
      .callsFake(
        (options: unknown) =>
          new HttpConnection({ ...client['getConnectionOptions'](options as ConnectionOptions), https: false }, client),
      );
    return client;
  }

  async function runSessionAndStatement(options: Partial<ConnectionOptions>) {
    const client = createClient();
    await client.connect({
      host: '127.0.0.1',
      port: server.port,
      path: '/sql/1.0/warehouses/abc',
      token: 'dapi-test-token',
      telemetryEnabled: false,
      ...options,
    } as ConnectionOptions);
    const session = await client.openSession();
    try {
      await session.executeStatement('SELECT 1 AS test_value');
      expect.fail('the fake server fails every statement');
    } catch (error) {
      expect((error as Error).message).to.contain('fake server');
    }
    await client.close();
    return {
      openSession: server.requests.find((r) => r.method === 'OpenSession'),
      executeStatement: server.requests.find((r) => r.method === 'ExecuteStatement'),
    };
  }

  it('sends custom headers on OpenSession and ExecuteStatement', async () => {
    const { openSession, executeStatement } = await runSessionAndStatement({
      customHeaders: {
        'X-Databricks-Spec-Header-A': 'session-017-a',
        'X-Databricks-Spec-Header-B': 'session-017-b',
      },
    });

    expect(openSession?.headers['x-databricks-spec-header-a']).to.equal('session-017-a');
    expect(openSession?.headers['x-databricks-spec-header-b']).to.equal('session-017-b');
    expect(executeStatement?.headers['x-databricks-spec-header-a']).to.equal('session-017-a');
    expect(executeStatement?.headers['x-databricks-spec-header-b']).to.equal('session-017-b');
  });

  it('keeps driver-managed headers authoritative over caller-supplied ones', async () => {
    const { openSession, executeStatement } = await runSessionAndStatement({
      userAgentEntry: 'MyApp/1.0',
      customHeaders: {
        authorization: 'Bearer caller-token',
        'USER-AGENT': 'Caller/1.0',
        'Content-Type': 'text/plain',
        'content-length': '1',
        'Transfer-Encoding': 'chunked',
        'X-Keep': 'kept',
      },
    });

    for (const request of [openSession, executeStatement]) {
      expect(request?.headers.authorization).to.equal('Bearer dapi-test-token');
      expect(request?.headers['user-agent']).to.match(/^NodejsDatabricksSqlConnector\/.*MyApp\/1\.0/);
      expect(request?.headers['content-type']).to.equal('application/x-thrift');
      expect(request?.headers['content-length']).to.not.equal('1');
      expect(request?.headers).to.not.have.property('transfer-encoding');
      expect(request?.headers['x-keep']).to.equal('kept');
    }
  });

  it('does not add the SPOG org-id header that is derived from the path', async () => {
    const { openSession } = await runSessionAndStatement({
      path: '/sql/1.0/warehouses/abc?o=1234567890',
      customHeaders: { 'X-Keep': 'kept' },
    });

    expect(openSession?.headers['x-keep']).to.equal('kept');
    expect(openSession?.headers).to.not.have.property('x-databricks-org-id');
  });

  it('forwards a caller-supplied x-databricks-org-id', async () => {
    const { openSession } = await runSessionAndStatement({
      customHeaders: { 'x-databricks-org-id': '42' },
    });

    expect(openSession?.headers['x-databricks-org-id']).to.equal('42');
  });

  it('lets custom-provider headers win over caller headers with different casing', async () => {
    const { openSession, executeStatement } = await runSessionAndStatement({
      authType: 'custom',
      provider: { authenticate: async () => ({ 'X-Auth-Token': 'new' }) },
      customHeaders: { 'x-auth-token': 'old', 'X-Keep': 'kept' },
    } as Partial<ConnectionOptions>);

    for (const request of [openSession, executeStatement]) {
      expect(request?.headers['x-auth-token']).to.equal('new');
      expect(request?.headers['x-keep']).to.equal('kept');
    }
  });

  it('rejects headers that cannot be sent, without echoing the value', async () => {
    const cases: Array<[Record<string, string>, RegExp]> = [
      [{ 'X-Bad': 'secret\r\nInjected: 1' }, /customHeaders value for `X-Bad`/],
      [{ 'X-Bad': 'secret\0' }, /customHeaders value for `X-Bad`/],
      [{ 'X Bad': 'secret' }, /customHeaders name "X Bad"/],
      [{ '': 'secret' }, /customHeaders name ""/],
    ];

    for (const [customHeaders, message] of cases) {
      const client = createClient();
      try {
        await client.connect({
          host: '127.0.0.1',
          port: server.port,
          path: '/sql/1.0/warehouses/abc',
          token: 'dapi-test-token',
          telemetryEnabled: false,
          customHeaders,
        });
        expect.fail('connect should reject an unsendable header');
      } catch (error) {
        expect(error).to.be.instanceOf(HiveDriverError);
        expect((error as Error).message).to.match(message);
        expect((error as Error).message).to.not.contain('secret');
      }
    }
    expect(server.requests).to.be.empty;
  });
});

describe('HttpConnection customHeaders', () => {
  it('merges custom headers under the driver and auth headers', async () => {
    const context = new ClientContextStub();
    const log = sinon.spy(context.logger, 'log');
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
        headers: { 'User-Agent': 'driver-ua' },
        customHeaders: { 'X-A': 'a', 'user-agent': 'caller-ua', AUTHORIZATION: 'caller-auth', Connection: 'close' },
      },
      context,
    );

    const thriftConnection = await connection.getThriftConnection();
    expect(thriftConnection.config.headers).to.deep.equal({ 'X-A': 'a', 'User-Agent': 'driver-ua' });

    connection.setHeaders({ Authorization: 'Bearer token' });
    expect(thriftConnection.config.headers).to.deep.equal({
      'X-A': 'a',
      'User-Agent': 'driver-ua',
      Authorization: 'Bearer token',
    });

    const warnings = log.getCalls().filter((call) => call.args[0] === LogLevel.warn);
    expect(warnings.map((call) => call.args[1])).to.have.length(3);
    expect(warnings[0].args[1]).to.contain('`user-agent`');
  });

  it('drops caller headers that driver or auth headers set in a different casing', async () => {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
        headers: { 'X-Driver': 'driver' },
        customHeaders: { 'x-driver': 'caller', 'x-auth-token': 'old', 'X-Keep': 'kept' },
      },
      new ClientContextStub(),
    );

    const thriftConnection = await connection.getThriftConnection();
    expect(thriftConnection.config.headers).to.deep.equal({
      'x-auth-token': 'old',
      'X-Keep': 'kept',
      'X-Driver': 'driver',
    });

    connection.setHeaders({ 'X-Auth-Token': 'new' });
    expect(thriftConnection.config.headers).to.deep.equal({
      'X-Keep': 'kept',
      'X-Driver': 'driver',
      'X-Auth-Token': 'new',
    });

    connection.setHeaders({});
    expect(thriftConnection.config.headers).to.deep.equal({
      'x-auth-token': 'old',
      'X-Keep': 'kept',
      'X-Driver': 'driver',
    });
  });
});
