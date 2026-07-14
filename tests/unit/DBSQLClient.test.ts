import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import DBSQLClient, { ThriftLibrary } from '../../lib/DBSQLClient';
import DBSQLSession from '../../lib/DBSQLSession';
import ThriftBackend from '../../lib/thrift-backend/ThriftBackend';

import PlainHttpAuthentication from '../../lib/connection/auth/PlainHttpAuthentication';
import DatabricksOAuth from '../../lib/connection/auth/DatabricksOAuth';
import { DatabricksOAuthManager, AzureOAuthManager } from '../../lib/connection/auth/DatabricksOAuth/OAuthManager';

import HttpConnection from '../../lib/connection/connections/HttpConnection';
import { ConnectionOptions } from '../../lib/contracts/IDBSQLClient';
import IRetryPolicy from '../../lib/connection/contracts/IRetryPolicy';
import IConnectionProvider, { HttpTransactionDetails } from '../../lib/connection/contracts/IConnectionProvider';
import ThriftClientStub from './.stubs/ThriftClientStub';
import IThriftClient from '../../lib/contracts/IThriftClient';
import IAuthentication from '../../lib/connection/contracts/IAuthentication';
import AuthProviderStub from './.stubs/AuthProviderStub';
import ConnectionProviderStub from './.stubs/ConnectionProviderStub';
import { TProtocolVersion } from '../../thrift/TCLIService_types';
import TelemetryClientProvider from '../../lib/telemetry/TelemetryClientProvider';
import FeatureFlagCache from '../../lib/telemetry/FeatureFlagCache';
import TelemetryEventEmitter from '../../lib/telemetry/TelemetryEventEmitter';
import { LogLevel } from '../../lib/contracts/IDBSQLLogger';

const connectOptions = {
  host: '127.0.0.1',
  port: 80,
  path: '',
  token: 'dapi********************************',
} satisfies ConnectionOptions;

// Test helper: build a DBSQLClient with `getClient` stubbed to return the given
// ThriftClient stub, and pre-seed `client['backend']` with a ThriftBackend.
// Used to avoid 12 copies of the same 4-line setup across the openSession tests.
function makeStubbedClient(thriftClient: ThriftClientStub = new ThriftClientStub()): {
  client: DBSQLClient;
  thriftClient: ThriftClientStub;
} {
  const client = new DBSQLClient();
  sinon.stub(client, 'getClient').returns(Promise.resolve(thriftClient));
  client['backend'] = new ThriftBackend({ context: client, onConnectionEvent: () => {} });
  return { client, thriftClient };
}

describe('DBSQLClient.connect', () => {
  it('should prepend "/" to path if it is missing', async () => {
    const client = new DBSQLClient();

    const path = 'example/path';
    const connectionOptions = client['getConnectionOptions']({ ...connectOptions, path });

    expect(connectionOptions.path).to.equal(`/${path}`);
  });

  it('should not prepend "/" to path if it is already available', async () => {
    const client = new DBSQLClient();

    const path = '/example/path';
    const connectionOptions = client['getConnectionOptions']({ ...connectOptions, path });

    expect(connectionOptions.path).to.equal(path);
  });

  it('should initialize connection state', async () => {
    const client = new DBSQLClient();

    expect(client['client']).to.be.undefined;
    expect(client['authProvider']).to.be.undefined;
    expect(client['connectionProvider']).to.be.undefined;

    await client.connect(connectOptions);

    expect(client['client']).to.be.undefined; // it should not be initialized at this point
    expect(client['authProvider']).to.be.instanceOf(PlainHttpAuthentication);
    expect(client['connectionProvider']).to.be.instanceOf(HttpConnection);
  });

  it('should listen for Thrift connection events', async () => {
    const client = new DBSQLClient();

    const thriftConnectionStub = {
      on: sinon.stub(),
    };

    // This method is private, so we cannot easily `sinon.stub` it.
    // But in this case we can just replace it
    client['createConnectionProvider'] = () => ({
      getThriftConnection: async () => thriftConnectionStub,
      getAgent: async () => undefined,
      setHeaders: () => {},
      getRetryPolicy: (): Promise<IRetryPolicy<HttpTransactionDetails>> => {
        throw new Error('Not implemented');
      },
    });

    await client.connect(connectOptions);

    expect(thriftConnectionStub.on.called).to.be.true;
  });

  it('should log a warning when deprecated clientId is passed', async () => {
    const client = new DBSQLClient();
    const logSpy = sinon.spy((client as any).logger, 'log');

    const optionsWithDeprecated = {
      ...connectOptions,
      clientId: 'clientId',
    };

    await client.connect(optionsWithDeprecated as any);

    const warningRegex = /Warning: The "clientId" option is deprecated\. Please use "userAgentEntry" instead\./;
    const callFound = logSpy.getCalls().some((call) => warningRegex.test(call.args[1]));

    expect(callFound).to.be.true;

    logSpy.restore();
  });

  it('useKernel: true routes to KernelBackend and leaves `backend` unset when connect() throws', async () => {
    const client = new DBSQLClient();

    // `useKernel` is on a non-exported InternalConnectionOptions; cast through any.
    // An empty token makes the real KernelBackend reject during connect() (auth
    // validation); where the native binding is absent (e.g. CI, which does not
    // build it) construction throws even earlier. Either way connect() must
    // reject, so we can assert the partial-init guard leaves `backend` unset.
    const kernelOptions = { ...connectOptions, token: '', useKernel: true } as any;

    try {
      await client.connect(kernelOptions);
      expect.fail('KernelBackend connect() should reject (empty PAT / absent native binding)');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      // The exact message differs by environment (auth rejection vs binding-load
      // failure); the contract under test is simply that connect() rejected.
    }

    // The partial-init guard (L2 fix) means backend stays undefined after a
    // failed connect, so the next openSession surfaces "not connected" rather
    // than the KernelBackend's own connect/auth error.
    expect((client as any).backend).to.equal(undefined);

    try {
      await client.openSession();
      expect.fail('openSession on an unconnected client should throw');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.match(/not connected/);
    }
  });

  it('populates config.customHeaders with org-id parsed from ?o= (SPOG)', async () => {
    const client = new DBSQLClient();
    await client.connect({ ...connectOptions, path: '/sql/1.0/warehouses/abc?o=12345678901234' });
    expect(client.getConfig().customHeaders).to.deep.equal({ 'x-databricks-org-id': '12345678901234' });
  });

  it('leaves config.customHeaders undefined when path has no ?o= and none supplied', async () => {
    const client = new DBSQLClient();
    await client.connect({ ...connectOptions, path: '/sql/1.0/warehouses/abc' });
    expect(client.getConfig().customHeaders).to.be.undefined;
  });
});

describe('DBSQLClient.openSession', () => {
  it('should successfully open session', async () => {
    const { client } = makeStubbedClient();

    const session = await client.openSession();
    expect(session).instanceOf(DBSQLSession);
  });

  it('should use initial namespace options', async () => {
    const { client, thriftClient } = makeStubbedClient();

    case1: {
      const initialCatalog = 'catalog1';
      const session = await client.openSession({ initialCatalog });
      expect(session).instanceOf(DBSQLSession);
      expect(thriftClient.openSessionReq?.initialNamespace?.catalogName).to.equal(initialCatalog);
      expect(thriftClient.openSessionReq?.initialNamespace?.schemaName).to.be.null;
    }

    case2: {
      const initialSchema = 'schema2';
      const session = await client.openSession({ initialSchema });
      expect(session).instanceOf(DBSQLSession);
      expect(thriftClient.openSessionReq?.initialNamespace?.catalogName).to.be.null;
      expect(thriftClient.openSessionReq?.initialNamespace?.schemaName).to.equal(initialSchema);
    }

    case3: {
      const initialCatalog = 'catalog3';
      const initialSchema = 'schema3';
      const session = await client.openSession({ initialCatalog, initialSchema });
      expect(session).instanceOf(DBSQLSession);
      expect(thriftClient.openSessionReq?.initialNamespace?.catalogName).to.equal(initialCatalog);
      expect(thriftClient.openSessionReq?.initialNamespace?.schemaName).to.equal(initialSchema);
    }
  });

  it('should throw an exception when not connected', async () => {
    const client = new DBSQLClient();
    client['backend'] = undefined;
    client['connectionProvider'] = undefined;

    try {
      await client.openSession();
      expect.fail('It should throw an error');
    } catch (error) {
      if (!(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.be.eq('DBSQLClient: not connected');
    }
  });

  it('should correctly pass server protocol version to session', async () => {
    const { client, thriftClient } = makeStubbedClient();

    // Test with default protocol version (SPARK_CLI_SERVICE_PROTOCOL_V8)
    {
      const session = await client.openSession();
      expect(session).instanceOf(DBSQLSession);
      expect(((session as DBSQLSession)['backend'] as any)['serverProtocolVersion']).to.equal(
        TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
      );
    }

    {
      thriftClient.openSessionResp = {
        ...thriftClient.openSessionResp,
        serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
      };

      const session = await client.openSession();
      expect(session).instanceOf(DBSQLSession);
      expect(((session as DBSQLSession)['backend'] as any)['serverProtocolVersion']).to.equal(
        TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
      );
    }
  });

  it('should pass session configuration to OpenSessionReq', async () => {
    const { client, thriftClient } = makeStubbedClient();

    const configuration = { QUERY_TAGS: 'team:engineering', ansi_mode: 'true' };
    await client.openSession({ configuration });
    expect(thriftClient.openSessionReq?.configuration).to.deep.equal(configuration);
  });

  it('should affect session behavior based on protocol version', async () => {
    const { client, thriftClient } = makeStubbedClient();

    // With protocol version V6 - should support async metadata operations
    {
      thriftClient.openSessionResp = {
        ...thriftClient.openSessionResp,
        serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6,
      };

      const session = await client.openSession();
      expect(session).instanceOf(DBSQLSession);

      // Spy on driver.getTypeInfo to check if runAsync is set
      const driver = await client.getDriver();
      const getTypeInfoSpy = sinon.spy(driver, 'getTypeInfo');

      await session.getTypeInfo();

      expect(getTypeInfoSpy.calledOnce).to.be.true;
      expect(getTypeInfoSpy.firstCall.args[0].runAsync).to.be.true;

      getTypeInfoSpy.restore();
    }

    // With protocol version V5 - should NOT support async metadata operations
    {
      thriftClient.openSessionResp = {
        ...thriftClient.openSessionResp,
        serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5,
      };

      const session = await client.openSession();
      expect(session).instanceOf(DBSQLSession);

      // Spy on driver.getTypeInfo to check if runAsync is undefined
      const driver = await client.getDriver();
      const getTypeInfoSpy = sinon.spy(driver, 'getTypeInfo');

      await session.getTypeInfo();

      expect(getTypeInfoSpy.calledOnce).to.be.true;
      expect(getTypeInfoSpy.firstCall.args[0].runAsync).to.be.undefined;

      getTypeInfoSpy.restore();
    }
  });
});

describe('DBSQLClient.getClient', () => {
  it('should throw an error if not connected', async () => {
    const client = new DBSQLClient();
    try {
      await client.getClient();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.contain('DBSQLClient: not connected');
    }
  });

  it('should create client if was not initialized yet', async () => {
    const client = new DBSQLClient();

    const thriftClient = new ThriftClientStub();
    const createThriftClient = sinon.stub().returns(thriftClient);

    client['authProvider'] = new AuthProviderStub();
    client['connectionProvider'] = new ConnectionProviderStub();
    client['thrift'] = {
      createClient: createThriftClient,
    };

    const result = await client.getClient();
    expect(createThriftClient.called).to.be.true;
    expect(result).to.be.equal(thriftClient);
  });

  it('should update auth credentials each time when client is requested', async () => {
    const client = new DBSQLClient();

    const thriftClient = new ThriftClientStub();
    const createThriftClient = sinon.stub().returns(thriftClient);
    const authProvider = sinon.spy(new AuthProviderStub());
    const connectionProvider = sinon.spy(new ConnectionProviderStub());

    client['connectionProvider'] = connectionProvider;
    client['thrift'] = {
      createClient: createThriftClient,
    };

    // just a sanity check - authProvider should not be initialized until `getClient()` call
    expect(client['authProvider']).to.be.undefined;
    expect(connectionProvider.setHeaders.callCount).to.be.equal(0);
    await client.getClient();
    expect(authProvider.authenticate.callCount).to.be.equal(0);
    expect(connectionProvider.setHeaders.callCount).to.be.equal(0);

    client['authProvider'] = authProvider;

    // initialize client
    firstCall: {
      const result = await client.getClient();
      expect(createThriftClient.callCount).to.be.equal(1);
      expect(connectionProvider.setHeaders.callCount).to.be.equal(1);
      expect(result).to.be.equal(thriftClient);
    }

    // credentials stay the same, client should not be re-created
    secondCall: {
      const result = await client.getClient();
      expect(createThriftClient.callCount).to.be.equal(1);
      expect(connectionProvider.setHeaders.callCount).to.be.equal(2);
      expect(result).to.be.equal(thriftClient);
    }

    // change credentials stub - client should be re-created
    thirdCall: {
      authProvider.headers = { test: 'test' };

      const result = await client.getClient();
      expect(createThriftClient.callCount).to.be.equal(1);
      expect(connectionProvider.setHeaders.callCount).to.be.equal(3);
      expect(result).to.be.equal(thriftClient);
    }
  });
});

describe('DBSQLClient.close', () => {
  it('should close the connection if it was initiated', async () => {
    const client = new DBSQLClient();
    client['client'] = new ThriftClientStub();
    client['connectionProvider'] = new ConnectionProviderStub();
    client['authProvider'] = new AuthProviderStub();

    await client.close();
    expect(client['client']).to.be.undefined;
    expect(client['connectionProvider']).to.be.undefined;
    expect(client['authProvider']).to.be.undefined;
  });

  it('should do nothing if the connection does not exist', async () => {
    const client = new DBSQLClient();

    expect(client['client']).to.be.undefined;
    expect(client['connectionProvider']).to.be.undefined;
    expect(client['authProvider']).to.be.undefined;

    await client.close();
    expect(client['client']).to.be.undefined;
    expect(client['connectionProvider']).to.be.undefined;
    expect(client['authProvider']).to.be.undefined;
  });

  it('should close sessions that belong to it', async () => {
    const client = new DBSQLClient();
    const thriftClient = sinon.spy(new ThriftClientStub());

    client['client'] = thriftClient;
    client['connectionProvider'] = new ConnectionProviderStub();
    client['authProvider'] = new AuthProviderStub();
    client['backend'] = new ThriftBackend({ context: client, onConnectionEvent: () => {} });

    const session = await client.openSession();
    if (!(session instanceof DBSQLSession)) {
      throw new Error('Assertion error: expected session to be DBSQLSession');
    }

    expect(session.onClose).to.be.not.undefined;
    expect(session['isOpen']).to.be.true;
    expect(client['sessions']['items'].size).to.eq(1);

    const closeAllSessionsSpy = sinon.spy(client['sessions'], 'closeAll');
    const sessionCloseSpy = sinon.spy(session, 'close');

    await client.close();
    expect(closeAllSessionsSpy.called).to.be.true;
    expect(sessionCloseSpy.called).to.be.true;
    expect(session.onClose).to.be.undefined;
    expect(session['isOpen']).to.be.false;
    expect(client['sessions']['items'].size).to.eq(0);
    expect(thriftClient.CloseSession.called).to.be.true;
  });
});

describe('DBSQLClient.createAuthProvider', () => {
  it('should use access token auth method', () => {
    const client = new DBSQLClient();

    const testAccessToken = 'token';
    const provider = client['createAuthProvider']({
      ...connectOptions,
      authType: 'access-token',
      token: testAccessToken,
    });

    expect(provider).to.be.instanceOf(PlainHttpAuthentication);
    if (!(provider instanceof PlainHttpAuthentication)) {
      throw new Error('Assertion error: expected provider to be PlainHttpAuthentication');
    }
    expect(provider['password']).to.be.equal(testAccessToken);
  });

  it('should use access token auth method by default (compatibility)', () => {
    const client = new DBSQLClient();

    const testAccessToken = 'token';
    const provider = client['createAuthProvider']({
      ...connectOptions,
      // note: no `authType` provided
      token: testAccessToken,
    });

    expect(provider).to.be.instanceOf(PlainHttpAuthentication);
    if (!(provider instanceof PlainHttpAuthentication)) {
      throw new Error('Assertion error: expected provider to be PlainHttpAuthentication');
    }
    expect(provider['password']).to.be.equal(testAccessToken);
  });

  it('should use Databricks OAuth method (AWS)', () => {
    const client = new DBSQLClient();

    const provider = client['createAuthProvider']({
      ...connectOptions,
      authType: 'databricks-oauth',
      // host is used when creating OAuth manager, so make it look like a real AWS instance
      host: 'example.dev.databricks.com',
      oauthClientSecret: 'test-secret',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
    if (!(provider instanceof DatabricksOAuth)) {
      throw new Error('Assertion error: expected provider to be DatabricksOAuth');
    }
    expect(provider['getManager']()).to.be.instanceOf(DatabricksOAuthManager);
  });

  it('should use Databricks OAuth method (Azure)', () => {
    const client = new DBSQLClient();

    const provider = client['createAuthProvider']({
      ...connectOptions,
      authType: 'databricks-oauth',
      // host is used when creating OAuth manager, so make it look like a real Azure instance
      host: 'example.databricks.azure.us',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
    if (!(provider instanceof DatabricksOAuth)) {
      throw new Error('Assertion error: expected provider to be DatabricksOAuth');
    }
    expect(provider['getManager']()).to.be.instanceOf(AzureOAuthManager);
  });

  it('should use Databricks OAuth method (GCP)', () => {
    const client = new DBSQLClient();

    const provider = client['createAuthProvider']({
      ...connectOptions,
      authType: 'databricks-oauth',
      // host is used when creating OAuth manager, so make it look like a real AWS instance
      host: 'example.gcp.databricks.com',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
    if (!(provider instanceof DatabricksOAuth)) {
      throw new Error('Assertion error: expected provider to be DatabricksOAuth');
    }
    expect(provider['getManager']()).to.be.instanceOf(DatabricksOAuthManager);
  });

  it('should use Databricks InHouse OAuth method (Azure)', () => {
    const client = new DBSQLClient();

    // When `useDatabricksOAuthInAzure = true`, it should use Databricks OAuth method
    // only for supported Azure hosts, and fail for others

    case1: {
      const provider = client['createAuthProvider']({
        ...connectOptions,
        authType: 'databricks-oauth',
        // host is used when creating OAuth manager, so make it look like a real Azure instance
        host: 'example.azuredatabricks.net',
        useDatabricksOAuthInAzure: true,
      });

      expect(provider).to.be.instanceOf(DatabricksOAuth);
      if (!(provider instanceof DatabricksOAuth)) {
        throw new Error('Assertion error: expected provider to be DatabricksOAuth');
      }
      expect(provider['getManager']()).to.be.instanceOf(DatabricksOAuthManager);
    }

    case2: {
      expect(() => {
        const provider = client['createAuthProvider']({
          ...connectOptions,
          authType: 'databricks-oauth',
          // host is used when creating OAuth manager, so make it look like a real Azure instance
          host: 'example.databricks.azure.us',
          useDatabricksOAuthInAzure: true,
        });

        if (!(provider instanceof DatabricksOAuth)) {
          throw new Error('Expected `provider` to be `DatabricksOAuth`');
        }
        provider['getManager'](); // just call the method
      }).to.throw();
    }
  });

  it('should throw error when OAuth not supported for host', () => {
    const client = new DBSQLClient();

    expect(() => {
      const provider = client['createAuthProvider']({
        ...connectOptions,
        authType: 'databricks-oauth',
        // use host which is not supported for sure
        host: 'example.com',
      });

      if (!(provider instanceof DatabricksOAuth)) {
        throw new Error('Expected `provider` to be `DatabricksOAuth`');
      }
      provider['getManager'](); // just call the method
    }).to.throw();
  });

  it('should use custom auth method', () => {
    const client = new DBSQLClient();

    const customProvider = {
      authenticate: () => Promise.resolve({}),
    };

    const provider = client['createAuthProvider']({
      ...connectOptions,
      authType: 'custom',
      provider: customProvider,
    });

    expect(provider).to.be.equal(customProvider);
  });

  it('should use custom auth method (legacy way)', () => {
    const client = new DBSQLClient();

    const customProvider = {
      authenticate: () => Promise.resolve({}),
    };

    const provider = client['createAuthProvider'](
      // custom provider from second arg should be used no matter what's specified in config
      { ...connectOptions, authType: 'access-token', token: 'token' },
      customProvider,
    );

    expect(provider).to.be.equal(customProvider);
  });
});

describe('DBSQLClient retry-policy ConnectionOptions', () => {
  it('ingests retry-policy options from connect() into ClientConfig', async () => {
    const client = new DBSQLClient();

    // Defaults before connect.
    expect(client.getConfig().retryMaxAttempts).to.equal(5);
    expect(client.getConfig().retriesTimeout).to.equal(15 * 60 * 1000);
    expect(client.getConfig().retryDelayMin).to.equal(1000);
    expect(client.getConfig().retryDelayMax).to.equal(60 * 1000);

    await client.connect({
      ...connectOptions,
      retryMaxAttempts: 2,
      retriesTimeout: 5000,
      retryDelayMin: 100,
      retryDelayMax: 500,
    });

    expect(client.getConfig().retryMaxAttempts).to.equal(2);
    expect(client.getConfig().retriesTimeout).to.equal(5000);
    expect(client.getConfig().retryDelayMin).to.equal(100);
    expect(client.getConfig().retryDelayMax).to.equal(500);
  });

  it('keeps defaults when retry-policy options are omitted', async () => {
    const client = new DBSQLClient();

    await client.connect({ ...connectOptions });

    expect(client.getConfig().retryMaxAttempts).to.equal(5);
    expect(client.getConfig().retriesTimeout).to.equal(15 * 60 * 1000);
    expect(client.getConfig().retryDelayMin).to.equal(1000);
    expect(client.getConfig().retryDelayMax).to.equal(60 * 1000);
  });
});

describe('DBSQLClient.enableMetricViewMetadata', () => {
  it('should store enableMetricViewMetadata config when enabled', async () => {
    const client = new DBSQLClient();

    expect(client.getConfig().enableMetricViewMetadata).to.be.undefined;

    await client.connect({ ...connectOptions, enableMetricViewMetadata: true });

    expect(client.getConfig().enableMetricViewMetadata).to.be.true;
  });

  it('should not store enableMetricViewMetadata config when disabled', async () => {
    const client = new DBSQLClient();

    expect(client.getConfig().enableMetricViewMetadata).to.be.undefined;

    await client.connect({ ...connectOptions, enableMetricViewMetadata: false });

    expect(client.getConfig().enableMetricViewMetadata).to.be.false;
  });

  it('should inject session parameter when enableMetricViewMetadata is true', async () => {
    const { client, thriftClient } = makeStubbedClient();

    await client.connect({ ...connectOptions, enableMetricViewMetadata: true });
    await client.openSession();

    expect(thriftClient.openSessionReq?.configuration).to.have.property(
      'spark.sql.thriftserver.metadata.metricview.enabled',
      'true',
    );
  });

  it('should not inject session parameter when enableMetricViewMetadata is false', async () => {
    const { client, thriftClient } = makeStubbedClient();

    await client.connect({ ...connectOptions, enableMetricViewMetadata: false });
    await client.openSession();

    expect(thriftClient.openSessionReq?.configuration).to.not.have.property(
      'spark.sql.thriftserver.metadata.metricview.enabled',
    );
  });

  it('should not inject session parameter when enableMetricViewMetadata is not set', async () => {
    const { client, thriftClient } = makeStubbedClient();

    await client.connect(connectOptions);
    await client.openSession();

    expect(thriftClient.openSessionReq?.configuration).to.not.have.property(
      'spark.sql.thriftserver.metadata.metricview.enabled',
    );
  });

  it('should preserve user-provided session configuration', async () => {
    const { client, thriftClient } = makeStubbedClient();

    await client.connect({ ...connectOptions, enableMetricViewMetadata: true });
    const userConfig = { QUERY_TAGS: 'team:engineering', ansi_mode: 'true' };
    await client.openSession({ configuration: userConfig });

    expect(thriftClient.openSessionReq?.configuration).to.deep.equal({
      ...userConfig,
      'spark.sql.thriftserver.metadata.metricview.enabled': 'true',
    });
  });

  it('should serialize queryTags dict and set in session configuration', async () => {
    const { client, thriftClient } = makeStubbedClient();

    await client.openSession({
      queryTags: { team: 'data-eng', project: 'etl' },
    });

    expect(thriftClient.openSessionReq?.configuration).to.deep.equal({
      QUERY_TAGS: 'team:data-eng,project:etl',
    });
  });

  it('should let queryTags take precedence over configuration.QUERY_TAGS', async () => {
    const { client, thriftClient } = makeStubbedClient();

    await client.openSession({
      queryTags: { team: 'new-team' },
      configuration: { QUERY_TAGS: 'team:old-team,other:value', ansi_mode: 'true' },
    });

    expect(thriftClient.openSessionReq?.configuration).to.deep.equal({
      QUERY_TAGS: 'team:new-team',
      ansi_mode: 'true',
    });
  });

  it('should remove QUERY_TAGS from configuration when queryTags is empty', async () => {
    const { client, thriftClient } = makeStubbedClient();

    await client.openSession({
      queryTags: {},
      configuration: { QUERY_TAGS: 'team:old-team', ansi_mode: 'true' },
    });

    expect(thriftClient.openSessionReq?.configuration).to.deep.equal({
      ansi_mode: 'true',
    });
  });
});

describe('DBSQLClient telemetry paths', () => {
  // Reset the process-wide singleton between tests so refcount + cached
  // feature flags from one test don't leak into the next. Mirrors the e2e
  // suite's afterEach pattern but scoped to unit-level state.
  afterEach(() => {
    TelemetryClientProvider.__resetInstanceForTests();
    sinon.restore();
  });

  describe('DATABRICKS_TELEMETRY_DISABLED env-var parser', () => {
    let savedEnv: string | undefined;
    beforeEach(() => {
      savedEnv = process.env.DATABRICKS_TELEMETRY_DISABLED;
    });
    afterEach(() => {
      if (savedEnv === undefined) {
        delete process.env.DATABRICKS_TELEMETRY_DISABLED;
      } else {
        process.env.DATABRICKS_TELEMETRY_DISABLED = savedEnv;
      }
    });

    const recognizedTruthy = ['1', 'true', 'TRUE', 'yes', 'YES', 'on', 'ON', ' true ', 'On'];
    const unrecognized = ['0', 'false', 'no', 'off', 'False', 'OFF'];

    for (const val of recognizedTruthy) {
      it(`DISABLES telemetry when value is '${val}'`, async () => {
        process.env.DATABRICKS_TELEMETRY_DISABLED = val;
        const client = new DBSQLClient();
        // Stub out initializeTelemetry so we can detect it not being called.
        const initSpy = sinon.spy(client as any, 'initializeTelemetry');
        await client.connect(connectOptions);
        expect(initSpy.callCount).to.equal(0);
      });
    }

    for (const val of unrecognized) {
      it(`WARNS and does NOT disable when value is '${val}'`, async () => {
        process.env.DATABRICKS_TELEMETRY_DISABLED = val;
        const client = new DBSQLClient();
        const logger = (client as any).logger as { log: (l: LogLevel, m: string) => void };
        const logSpy = sinon.spy(logger, 'log');
        // Suppress the actual telemetry init network calls by stubbing.
        sinon.stub(client as any, 'initializeTelemetry').resolves();
        await client.connect(connectOptions);
        const warnCalls = logSpy
          .getCalls()
          .filter((c) => c.args[0] === LogLevel.warn && /DATABRICKS_TELEMETRY_DISABLED/.test(c.args[1] as string));
        expect(warnCalls.length, 'expected a warn-level log about ignored env value').to.be.at.least(1);
      });
    }

    it('treats empty / unset env var as no-op (no warn)', async () => {
      delete process.env.DATABRICKS_TELEMETRY_DISABLED;
      const client = new DBSQLClient();
      const logger = (client as any).logger as { log: (l: LogLevel, m: string) => void };
      const logSpy = sinon.spy(logger, 'log');
      sinon.stub(client as any, 'initializeTelemetry').resolves();
      await client.connect(connectOptions);
      const warnCalls = logSpy
        .getCalls()
        .filter((c) => c.args[0] === LogLevel.warn && /DATABRICKS_TELEMETRY_DISABLED/.test(c.args[1] as string));
      expect(warnCalls.length).to.equal(0);
    });
  });

  describe('extractWorkspaceId', () => {
    it('returns the numeric o= param from httpPath', () => {
      const id = (DBSQLClient as any).extractWorkspaceId('/sql/1.0/warehouses/abc?o=12345678901234');
      expect(id).to.equal('12345678901234');
    });

    it('returns undefined when no query string', () => {
      expect((DBSQLClient as any).extractWorkspaceId('/sql/1.0/warehouses/abc')).to.be.undefined;
    });

    it('returns undefined when o= is not numeric', () => {
      expect((DBSQLClient as any).extractWorkspaceId('/sql/1.0/warehouses/abc?o=tenant_xyz')).to.be.undefined;
    });

    it('handles o= as a non-first param', () => {
      expect((DBSQLClient as any).extractWorkspaceId('/sql/1.0/warehouses/abc?foo=bar&o=42&baz=qux')).to.equal('42');
    });

    it('returns undefined when httpPath is unset', () => {
      expect((DBSQLClient as any).extractWorkspaceId(undefined)).to.be.undefined;
    });

    it('returns the numeric workspace id from all-purpose cluster path form', () => {
      expect((DBSQLClient as any).extractWorkspaceId('sql/protocolv1/o/99999999999999/0101-000000-aaaaaaaa')).to.equal(
        '99999999999999',
      );
    });

    it('returns the numeric workspace id from all-purpose cluster path with leading slash', () => {
      expect((DBSQLClient as any).extractWorkspaceId('/sql/protocolv1/o/12345/0101-000000-aaaaaaaa')).to.equal('12345');
    });

    it('returns undefined when all-purpose cluster path has non-numeric workspace segment', () => {
      expect((DBSQLClient as any).extractWorkspaceId('sql/protocolv1/o/tenant_xyz/0101-000000-aaaaaaaa')).to.be
        .undefined;
    });

    it('prefers ?o= query form over /o/ path form when both are present', () => {
      expect((DBSQLClient as any).extractWorkspaceId('sql/protocolv1/o/111/cluster?o=222')).to.equal('222');
    });
  });

  describe('buildCustomHeaders (SPOG)', () => {
    it('injects x-databricks-org-id from ?o= in httpPath', () => {
      const client = new DBSQLClient();
      const headers = (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc?o=12345678901234', undefined);
      expect(headers).to.deep.equal({ 'x-databricks-org-id': '12345678901234' });
    });

    it('returns undefined when no ?o= and no user-supplied customHeaders', () => {
      const client = new DBSQLClient();
      const headers = (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc', undefined);
      expect(headers).to.be.undefined;
    });

    it('preserves user-supplied customHeaders alongside parsed org-id', () => {
      const client = new DBSQLClient();
      const headers = (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc?o=42', { 'x-trace-id': 'tid-001' });
      expect(headers).to.deep.equal({ 'x-trace-id': 'tid-001', 'x-databricks-org-id': '42' });
    });

    it('user-supplied x-databricks-org-id wins over ?o= parsed value (case-insensitive)', () => {
      const client = new DBSQLClient();
      const headers = (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc?o=42', {
        'X-Databricks-Org-Id': '999',
      });
      expect(headers).to.deep.equal({ 'X-Databricks-Org-Id': '999' });
    });

    it('does not inject org-id when ?o= value is non-numeric', () => {
      const client = new DBSQLClient();
      const headers = (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc?o=tenant_xyz', undefined);
      expect(headers).to.be.undefined;
    });

    it('injects x-databricks-org-id from all-purpose cluster path form', () => {
      const client = new DBSQLClient();
      const headers = (client as any).buildCustomHeaders(
        'sql/protocolv1/o/99999999999999/0101-000000-aaaaaaaa',
        undefined,
      );
      expect(headers).to.deep.equal({ 'x-databricks-org-id': '99999999999999' });
    });

    it('logs a warning when workspace ID segment is non-numeric (path form)', () => {
      const client = new DBSQLClient();
      const logSpy = sinon.spy((client as any).logger, 'log');
      try {
        (client as any).buildCustomHeaders('sql/protocolv1/o/tenant_xyz/cluster', undefined);
        const warnCalls = logSpy.getCalls().filter((c) => c.args[0] === LogLevel.warn);
        expect(warnCalls).to.have.lengthOf(1);
        expect(warnCalls[0].args[1]).to.match(/non-numeric workspace ID/);
      } finally {
        logSpy.restore();
      }
    });

    it('logs a warning when ?o= is present but non-numeric', () => {
      const client = new DBSQLClient();
      const logSpy = sinon.spy((client as any).logger, 'log');
      try {
        (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc?o=tenant_xyz', undefined);
        const warnCalls = logSpy.getCalls().filter((c) => c.args[0] === LogLevel.warn);
        expect(warnCalls).to.have.lengthOf(1);
        expect(warnCalls[0].args[1]).to.match(/non-numeric workspace ID/);
      } finally {
        logSpy.restore();
      }
    });

    it('logs a debug line when injecting org-id from httpPath', () => {
      const client = new DBSQLClient();
      const logSpy = sinon.spy((client as any).logger, 'log');
      try {
        (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc?o=42', undefined);
        const injectLog = logSpy
          .getCalls()
          .find((c) => c.args[0] === LogLevel.debug && /injecting x-databricks-org-id=42/.test(String(c.args[1])));
        expect(injectLog, 'expected SPOG inject debug log').to.exist;
      } finally {
        logSpy.restore();
      }
    });

    it('logs a debug line when caller supplies x-databricks-org-id', () => {
      const client = new DBSQLClient();
      const logSpy = sinon.spy((client as any).logger, 'log');
      try {
        (client as any).buildCustomHeaders('/sql/1.0/warehouses/abc?o=42', { 'x-databricks-org-id': '999' });
        const callerLog = logSpy
          .getCalls()
          .find((c) => c.args[0] === LogLevel.debug && /supplied by caller/.test(String(c.args[1])));
        expect(callerLog, 'expected SPOG caller-supplied debug log').to.exist;
      } finally {
        logSpy.restore();
      }
    });
  });

  describe('telemetry refcount on reconnect', () => {
    it('releases the prior refcount when connect() is called twice', async () => {
      const client = new DBSQLClient();
      // Stub out feature-flag fetch to return true so the telemetry path runs.
      sinon.stub(FeatureFlagCache.prototype, 'isTelemetryEnabled').resolves(true);
      const releaseSpy = sinon.spy(TelemetryClientProvider.prototype, 'releaseClient');

      await client.connect(connectOptions);
      // Sanity: refcount on host should be 1 after first connect.
      expect(TelemetryClientProvider.getInstance().getRefCount(connectOptions.host)).to.equal(1);

      // Second connect to a different host should release the prior refcount.
      await client.connect({ ...connectOptions, host: '127.0.0.2' });
      expect(releaseSpy.called, 'releaseClient should fire on reconnect').to.be.true;
      // The old host should have decremented to 0 (closed and removed).
      expect(TelemetryClientProvider.getInstance().getRefCount(connectOptions.host)).to.equal(0);

      await client.close();
    });
  });

  describe('telemetry refcount release path on init failure', () => {
    it('releases refcount when feature flag fetch throws', async () => {
      const client = new DBSQLClient();
      sinon.stub(FeatureFlagCache.prototype, 'isTelemetryEnabled').rejects(new Error('boom'));
      const releaseSpy = sinon.spy(TelemetryClientProvider.prototype, 'releaseClient');

      await client.connect(connectOptions);

      // The init failure should release the refcount it just acquired so the
      // per-host TelemetryClient doesn't leak its flush timer for the
      // lifetime of the process.
      expect(releaseSpy.called, 'releaseClient should run on init failure').to.be.true;
      expect(TelemetryClientProvider.getInstance().getRefCount(connectOptions.host)).to.equal(0);
    });
  });

  describe('CONNECTION_OPEN driverConfig de-duplication (F15)', () => {
    it('ships driverConfig on the first openSession only', async () => {
      const client = new DBSQLClient();
      const thriftClient = new ThriftClientStub();
      sinon.stub(client, 'getClient').returns(Promise.resolve(thriftClient));
      // Need a working emitter so we can spy on emitConnectionOpen.
      sinon.stub(FeatureFlagCache.prototype, 'isTelemetryEnabled').resolves(true);

      const emitSpy = sinon.spy(TelemetryEventEmitter.prototype, 'emitConnectionOpen');

      await client.connect(connectOptions);
      await client.openSession();
      await client.openSession();
      await client.openSession();

      // 3 sessions → 3 emitConnectionOpen calls, but only the first one
      // should carry a non-undefined driverConfig blob.
      const calls = emitSpy.getCalls();
      expect(calls.length, 'three openSession should emit three CONNECTION_OPEN events').to.equal(3);
      expect(calls[0].args[0].driverConfig, 'first session ships full driverConfig').to.not.be.undefined;
      expect(calls[1].args[0].driverConfig, 'second session omits driverConfig').to.be.undefined;
      expect(calls[2].args[0].driverConfig, 'third session omits driverConfig').to.be.undefined;

      await client.close();
    });
  });

  describe('getTelemetryStats', () => {
    it('returns undefined when telemetry is disabled', async () => {
      const client = new DBSQLClient();
      await client.connect({ ...connectOptions, telemetryEnabled: false });
      expect(client.getTelemetryStats()).to.be.undefined;
    });

    it('returns a populated snapshot when telemetry is enabled', async () => {
      const client = new DBSQLClient();
      sinon.stub(FeatureFlagCache.prototype, 'isTelemetryEnabled').resolves(true);
      await client.connect(connectOptions);
      const stats = client.getTelemetryStats();
      expect(stats, 'stats should be populated when telemetry is on').to.not.be.undefined;
      expect(stats!.host).to.equal(connectOptions.host);
      expect(stats!.pendingMetricsCount).to.be.a('number');
      expect(stats!.droppedMetrics).to.be.a('number');
      expect(stats!.evictedStatements).to.be.a('number');
      expect(stats!.circuitBreakerState).to.be.oneOf(['CLOSED', 'OPEN', 'HALF_OPEN']);
      await client.close();
    });
  });
});
