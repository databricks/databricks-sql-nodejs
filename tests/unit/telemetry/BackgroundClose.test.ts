import { expect } from 'chai';
import sinon from 'sinon';
import http from 'http';
import { AddressInfo } from 'net';
import fetch, { RequestInfo, RequestInit } from 'node-fetch';
import { Issuer } from 'openid-client';
import DBSQLClient from '../../../lib/DBSQLClient';
import FeatureFlagCache from '../../../lib/FeatureFlagCache';
import FederationProvider, {
  setFederationFetchForTest,
} from '../../../lib/connection/auth/tokenProvider/FederationProvider';
import OAuthToken from '../../../lib/connection/auth/DatabricksOAuth/OAuthToken';
import TelemetryClientProvider from '../../../lib/telemetry/TelemetryClientProvider';
import TelemetryClient from '../../../lib/telemetry/TelemetryClient';
import { TelemetryEventType } from '../../../lib/telemetry/types';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createAccessToken } from '../.stubs/OAuth';

describe('background telemetry close', () => {
  const host = 'close-test.cloud.databricks.com';
  let clock: sinon.SinonFakeTimers;
  let provider: TelemetryClientProvider;

  function addConnection(client: TelemetryClient, sessionId: string = 'session-1'): void {
    client.getAggregator().processEvent({
      eventType: TelemetryEventType.CONNECTION_OPEN,
      timestamp: Date.now(),
      sessionId,
    });
  }

  function delayedRequest(client: TelemetryClient, delay: number = 50): sinon.SinonStub {
    return sinon.stub(client.getExporter() as any, 'sendRequest').callsFake(
      () =>
        new Promise((resolve) => {
          setTimeout(() => resolve({ ok: true, text: async () => '' }), delay);
        }),
    );
  }

  function makeContext(): ClientContextStub {
    const context = new ClientContextStub({ telemetryCloseTimeoutMs: 100, telemetryMaxRetries: 0 });
    sinon.stub(context, 'getAuthProvider').returns({ authenticate: async () => ({ Authorization: 'Bearer mock' }) });
    return context;
  }

  beforeEach(() => {
    clock = sinon.useFakeTimers();
    TelemetryClientProvider.__resetInstanceForTests();
    provider = TelemetryClientProvider.getInstance();
  });

  afterEach(() => {
    sinon.restore();
    clock.restore();
    TelemetryClientProvider.__resetInstanceForTests();
  });

  it('preserves the awaited default and explicit true', async () => {
    for (const wait of [undefined, true]) {
      const context = makeContext();
      const client = provider.getOrCreateClient(context, host);
      const request = delayedRequest(client);
      addConnection(client);
      let released = false;
      const closing = provider.releaseClient(context, host, wait).then(() => {
        released = true;
      });
      await clock.tickAsync(49);
      expect(released).to.equal(false);
      await clock.tickAsync(1);
      await closing;
      expect(request.calledOnce).to.equal(true);
    }
    expect(clock.countTimers()).to.equal(0);
  });

  it('detaches registry, auth and transport before DBSQLClient.close returns', async () => {
    const context = makeContext();
    const client = new DBSQLClient({ logger: context.logger });
    Object.assign(client as any, {
      host,
      authProvider: context.getAuthProvider(),
      connectionProvider: context.connectionProvider,
    });
    const shared = provider.getOrCreateClient(client, host);
    (client as any).telemetryClient = shared;
    const request = delayedRequest(shared);
    addConnection(shared);

    await client.close({ waitForTelemetry: false });

    expect(provider.getActiveClients().size).to.equal(0);
    expect(shared.isClosed()).to.equal(true);
    expect((client as any).authProvider).to.equal(undefined);
    expect((client as any).connectionProvider).to.equal(undefined);
    expect(await (shared.getExporter() as any).connectionProvider).to.equal(context.connectionProvider);
    expect((shared.getExporter() as any).authProvider).to.equal(context.getAuthProvider());
    await clock.tickAsync(50);
    expect(request.calledOnce).to.equal(true);
    await client.close({ waitForTelemetry: false });
    expect(clock.countTimers()).to.equal(0);
  });

  for (const options of [null, 0, { waitForTelemetry: null }, { waitForTelemetry: 0 }, { waitForTelemetry: '' }]) {
    it(`requires explicit false to detach telemetry: ${JSON.stringify(options)}`, async () => {
      const context = makeContext();
      const client = new DBSQLClient({ logger: context.logger });
      Object.assign(client, {
        host,
        authProvider: context.getAuthProvider(),
        connectionProvider: context.connectionProvider,
      });
      const shared = provider.getOrCreateClient(client, host);
      Object.assign(client, { telemetryClient: shared });
      const request = delayedRequest(shared);
      addConnection(shared);
      let closed = false;
      const closing = client.close(options as any).then(() => {
        closed = true;
      });
      await clock.tickAsync(49);
      expect(closed).to.equal(false);
      expect(provider.getRefCount(host)).to.equal(0);
      await clock.tickAsync(1);
      await closing;
      expect(request.calledOnce).to.equal(true);
      expect(shared.isClosed()).to.equal(true);
      expect(provider.getActiveClients().size).to.equal(0);
      expect(clock.countTimers()).to.equal(0);
    });
  }

  it('leaves other registrations active and close/reopen uses independent exporters', async () => {
    const first = makeContext();
    const second = makeContext();
    const shared = provider.getOrCreateClient(first, host);
    provider.getOrCreateClient(second, host);
    const firstRequest = delayedRequest(shared);
    addConnection(shared);
    await provider.releaseClient(first, host, false);
    expect(shared.isClosed()).to.equal(false);
    expect(provider.getRefCount(host)).to.equal(1);
    expect(firstRequest.called).to.equal(false);

    await provider.releaseClient(second, host, false);
    const reopened = provider.getOrCreateClient(first, host);
    const secondRequest = delayedRequest(reopened);
    expect(reopened).not.to.equal(shared);
    expect(reopened.isClosed()).to.equal(false);
    addConnection(reopened, 'session-2');
    await provider.releaseClient(first, host, false);
    await clock.tickAsync(50);
    expect(firstRequest.calledOnce).to.equal(true);
    expect(secondRequest.calledOnce).to.equal(true);
    expect(provider.getActiveClients().size).to.equal(0);
    expect(clock.countTimers()).to.equal(0);
  });

  it('bounds detached exporters and waits when the background queue is full', async () => {
    for (let index = 0; index < 32; index += 1) {
      const context = makeContext();
      const client = provider.getOrCreateClient(context, host);
      delayedRequest(client);
      addConnection(client);
      await provider.releaseClient(context, host, false);
    }
    expect((provider as any).pendingCloses.size).to.equal(32);
    const context = makeContext();
    const client = provider.getOrCreateClient(context, host);
    delayedRequest(client);
    addConnection(client);
    let released = false;
    const closing = provider.releaseClient(context, host, false).then(() => {
      released = true;
    });
    await clock.tickAsync(49);
    expect(released).to.equal(false);
    expect((provider as any).pendingCloses.size).to.equal(32);
    await clock.tickAsync(1);
    await closing;
    expect((provider as any).pendingCloses.size).to.equal(0);
    expect(clock.countTimers()).to.equal(0);
  });

  it('drains an in-flight batch and the metrics queued during that batch', async () => {
    const context = makeContext();
    context.configOverrides.telemetryBatchSize = 1;
    const client = provider.getOrCreateClient(context, host);
    const request = delayedRequest(client, 20);
    addConnection(client, 'first');
    await clock.tickAsync(0);
    addConnection(client, 'second');
    await provider.releaseClient(context, host, false);
    await clock.tickAsync(40);
    expect(request.callCount).to.equal(2);
    const sessions = request.getCalls().map((call) => {
      const body = JSON.parse(call.args[1].body);
      return JSON.parse(body.protoLogs[0]).entry.sql_driver_log.session_id;
    });
    expect(sessions).to.deep.equal(['first', 'second']);
    expect(clock.countTimers()).to.equal(0);
  });

  it('aborts a stalled export at the deadline without error logs or leaked timers', async () => {
    const context = makeContext();
    const log = sinon.spy(context.logger, 'log');
    const client = provider.getOrCreateClient(context, host);
    let signal: AbortSignal | undefined;
    const request = sinon.stub(client.getExporter() as any, 'sendRequest').callsFake(
      (_url: unknown, init: any) =>
        new Promise((_resolve, reject) => {
          signal = init.signal;
          signal!.addEventListener('abort', () => reject(new Error('cancelled')), { once: true });
        }),
    );
    addConnection(client);
    await provider.releaseClient(context, host, false);
    await clock.tickAsync(99);
    expect(signal?.aborted).to.equal(false);
    await clock.tickAsync(1);
    expect(signal?.aborted).to.equal(true);
    expect(clock.countTimers()).to.equal(0);
    expect(log.getCalls().some((call) => String(call.args[1]).startsWith('Telemetry export error:'))).to.equal(false);
    await clock.tickAsync(60000);
    expect(request.calledOnce).to.equal(true);
    expect(log.getCalls().filter((call) => call.args[0] === LogLevel.error)).to.have.length(0);
    expect((provider as any).pendingCloses.size).to.equal(0);
    expect(clock.countTimers()).to.equal(0);
  });

  it('cancels retry backoff when the background deadline expires', async () => {
    const context = makeContext();
    Object.assign(context.configOverrides, { telemetryMaxRetries: 3, telemetryBackoffBaseMs: 1000 });
    const client = provider.getOrCreateClient(context, host);
    const request = sinon.stub(client.getExporter() as any, 'sendRequest').resolves({
      ok: false,
      status: 503,
      statusText: 'Unavailable',
      text: async () => '',
    });
    addConnection(client);
    await provider.releaseClient(context, host, false);
    await clock.tickAsync(100);
    expect(clock.countTimers()).to.equal(0);
    await clock.tickAsync(60000);
    expect(request.calledOnce).to.equal(true);
    expect(clock.countTimers()).to.equal(0);
    expect((provider as any).pendingCloses.size).to.equal(0);
  });

  it('handles a rejected detached transport without an unhandled rejection', async () => {
    const context = makeContext();
    const unhandled: unknown[] = [];
    const onRejection = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on('unhandledRejection', onRejection);
    sinon
      .stub(context.getAuthProvider()!, 'authenticate')
      .callsFake(() => new Promise((resolve) => setTimeout(() => resolve({ Authorization: 'Bearer mock' }), 50)));
    sinon.stub(context, 'getConnectionProvider').rejects(new Error('transport unavailable'));
    const client = provider.getOrCreateClient(context, host);
    addConnection(client);
    try {
      await provider.releaseClient(context, host, false);
      await clock.tickAsync(25);
      expect(unhandled).to.deep.equal([]);
      await clock.tickAsync(75);
      expect(unhandled).to.deep.equal([]);
      expect((provider as any).pendingCloses.size).to.equal(0);
      expect(clock.countTimers()).to.equal(0);
    } finally {
      process.off('unhandledRejection', onRejection);
    }
  });
});

describe('background close built-in authentication', () => {
  const host = 'close-auth.cloud.databricks.com';
  let server: http.Server;
  let endpoint: string;
  let client: DBSQLClient;
  let requests: Array<{ path?: string; authorization?: string; body: string }>;
  let finishAuth: () => void;
  let authenticated: Promise<void>;
  const refreshedToken = createAccessToken(Math.floor(Date.now() / 1000) + 3600);

  beforeEach(async () => {
    TelemetryClientProvider.__resetInstanceForTests();
    sinon.stub(FeatureFlagCache.prototype as any, 'fetchFeatureFlags').resolves(new Map());
    sinon.stub(FeatureFlagCache.prototype, 'getBoolean').resolves(true);
    authenticated = new Promise<void>((resolve) => {
      finishAuth = resolve;
    });
    requests = [];
    server = http.createServer((request, response) => {
      let body = '';
      request.on('data', (chunk) => {
        body += chunk;
      });
      request.on('end', () => {
        requests.push({ path: request.url, authorization: request.headers.authorization, body });
        response.setHeader('Content-Type', 'application/json');
        response.end(
          request.url === '/oidc/v1/token'
            ? JSON.stringify({ access_token: refreshedToken, refresh_token: 'next-refresh', token_type: 'Bearer' })
            : '{}',
        );
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    client = new DBSQLClient({ logger: new ClientContextStub().logger });
  });

  afterEach(async () => {
    finishAuth();
    await client.close();
    await Promise.all([...(TelemetryClientProvider.getInstance() as any).pendingCloses]);
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    setFederationFetchForTest();
    sinon.restore();
    TelemetryClientProvider.__resetInstanceForTests();
  });

  async function closeAndExport(): Promise<void> {
    const shared = (client as any).telemetryClient as TelemetryClient;
    expect(shared).not.to.equal(undefined);
    const exporter = shared.getExporter() as any;
    const sendRequest = exporter.sendRequest.bind(exporter);
    sinon
      .stub(exporter, 'sendRequest')
      .callsFake((_url: unknown, init: unknown) => sendRequest(`${endpoint}/telemetry-ext`, init));
    shared.getAggregator().processEvent({
      eventType: TelemetryEventType.CONNECTION_OPEN,
      timestamp: Date.now(),
      sessionId: 'built-in-auth',
    });
    await client.close({ waitForTelemetry: false });
    expect((client as any).connectionProvider).to.equal(undefined);
    finishAuth();
    await Promise.all([...(TelemetryClientProvider.getInstance() as any).pendingCloses]);
    expect(requests.map((request) => request.path)).to.deep.equal(['/oidc/v1/token', '/telemetry-ext']);
    expect(requests[1].authorization).to.equal(`Bearer ${refreshedToken}`);
  }

  it('exchanges a foreign JWT after close instead of exporting the original token', async () => {
    const getToken = FederationProvider.prototype.getToken;
    sinon
      .stub(FederationProvider.prototype, 'getToken')
      .callsFake(async function delayedFederation(this: FederationProvider) {
        await authenticated;
        return getToken.call(this);
      });
    const externalToken = `header.${Buffer.from(JSON.stringify({ iss: 'https://external-idp.example' })).toString(
      'base64url',
    )}.signature`;
    await client.connect({
      host,
      path: '/',
      authType: 'static-token',
      staticToken: externalToken,
      enableTokenFederation: true,
    });
    const transport = await client.getConnectionProvider();
    const federationFetch = async (url: RequestInfo, init?: RequestInit) => {
      expect(init?.agent).to.equal(await transport.getAgent(String(url)));
      return fetch(`${endpoint}/oidc/v1/token`, { ...init, agent: await transport.getAgent(endpoint) });
    };
    setFederationFetchForTest(federationFetch as unknown as typeof fetch);
    await closeAndExport();
    expect(new URLSearchParams(requests[0].body).get('subject_token')).to.equal(externalToken);
  });

  for (const oauthClientSecret of [undefined, 'mock-secret']) {
    it(`refreshes expired OAuth ${oauthClientSecret ? 'M2M' : 'U2M'} after close`, async () => {
      sinon
        .stub(Issuer, 'discover')
        .resolves(new Issuer({ issuer: `https://${host}/oidc`, token_endpoint: `${endpoint}/oidc/v1/token` }));
      await client.connect({
        host,
        path: '/',
        authType: 'databricks-oauth',
        oauthClientId: 'mock-client',
        oauthClientSecret,
        persistence: {
          read: async () => {
            await authenticated;
            return new OAuthToken(createAccessToken(1), 'previous-refresh');
          },
          persist: async () => {},
        },
      });
      await closeAndExport();
      expect(new URLSearchParams(requests[0].body).get('grant_type')).to.equal(
        oauthClientSecret ? 'client_credentials' : 'refresh_token',
      );
    });
  }
});

describe('background close HTTP transport', () => {
  it('uses the detached transport and credentials after the owning client is cleared', async () => {
    let finishAuth!: () => void;
    const authenticated = new Promise<void>((resolve) => {
      finishAuth = resolve;
    });
    let received!: (value: { authorization?: string; body: string }) => void;
    const exported = new Promise<{ authorization?: string; body: string }>((resolve) => {
      received = resolve;
    });
    const server = http.createServer((request, response) => {
      let body = '';
      request.on('data', (chunk) => {
        body += chunk;
      });
      request.on('end', () => {
        response.end('{}');
        received({ authorization: request.headers.authorization, body });
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const provider = new TelemetryClientProvider();
    const context = new ClientContextStub();
    const client = new DBSQLClient({ logger: context.logger });
    const agent = sinon.spy(context.connectionProvider, 'getAgent');
    Object.assign(client as any, {
      host: 'detached.cloud.databricks.com',
      authProvider: {
        authenticate: async () => {
          await authenticated;
          return { Authorization: 'Bearer before-close' };
        },
      },
      connectionProvider: context.connectionProvider,
    });
    const shared = provider.getOrCreateClient(client, 'detached.cloud.databricks.com');
    const exporter = shared.getExporter() as any;
    const sendRequest = exporter.sendRequest.bind(exporter);
    const stub = sinon
      .stub(exporter, 'sendRequest')
      .callsFake((_url: unknown, init: unknown) =>
        sendRequest(`http://127.0.0.1:${(server.address() as AddressInfo).port}/telemetry-ext`, init),
      );
    shared.getAggregator().processEvent({
      eventType: TelemetryEventType.CONNECTION_OPEN,
      sessionId: 'detached-session',
      timestamp: Date.now(),
    });
    try {
      await provider.releaseClient(client, 'detached.cloud.databricks.com', false);
      Object.assign(client as any, { authProvider: undefined, connectionProvider: undefined });
      finishAuth();
      const request = await exported;
      expect(request.authorization).to.equal('Bearer before-close');
      const body = JSON.parse(request.body);
      expect(JSON.parse(body.protoLogs[0]).entry.sql_driver_log.session_id).to.equal('detached-session');
      expect(agent.calledOnce).to.equal(true);
      const pending = [...(provider as any).pendingCloses] as Promise<void>[];
      await Promise.all(pending);
    } finally {
      stub.restore();
      agent.restore();
      finishAuth();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it('does not start a request when authentication finishes after the close deadline', async () => {
    let requests = 0;
    const server = http.createServer((_request, response) => {
      requests += 1;
      response.end('{}');
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    let finishAuth!: () => void;
    const authenticated = new Promise<void>((resolve) => {
      finishAuth = resolve;
    });
    const context = new ClientContextStub({ telemetryCloseTimeoutMs: 10 });
    const auth = sinon.stub(context, 'getAuthProvider').returns({
      authenticate: async () => {
        await authenticated;
        return { Authorization: 'Bearer late' };
      },
    });
    const provider = new TelemetryClientProvider();
    const shared = provider.getOrCreateClient(context, 'late.cloud.databricks.com');
    const exporter = shared.getExporter() as any;
    const sendRequest = exporter.sendRequest.bind(exporter);
    const stub = sinon
      .stub(exporter, 'sendRequest')
      .callsFake((_url: unknown, init: unknown) =>
        sendRequest(`http://127.0.0.1:${(server.address() as AddressInfo).port}/telemetry-ext`, init),
      );
    shared.getAggregator().processEvent({ eventType: TelemetryEventType.CONNECTION_OPEN, timestamp: Date.now() });
    try {
      await provider.releaseClient(context, 'late.cloud.databricks.com', false);
      await Promise.all([...(provider as any).pendingCloses]);
      finishAuth();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(requests).to.equal(0);
    } finally {
      finishAuth();
      stub.restore();
      auth.restore();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
