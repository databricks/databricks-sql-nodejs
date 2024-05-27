import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import DBSQLClient, { ThriftLibrary } from '../../lib/DBSQLClient';
import DBSQLSession from '../../lib/DBSQLSession';

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

const connectOptions = {
  host: '127.0.0.1',
  port: 80,
  path: '',
  token: 'dapi********************************',
} satisfies ConnectionOptions;

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
});

describe('DBSQLClient.openSession', () => {
  it('should successfully open session', async () => {
    const client = new DBSQLClient();
    const thriftClient = new ThriftClientStub();
    sinon.stub(client, 'getClient').returns(Promise.resolve(thriftClient));

    const session = await client.openSession();
    expect(session).instanceOf(DBSQLSession);
  });

  it('should use initial namespace options', async () => {
    const client = new DBSQLClient();
    const thriftClient = new ThriftClientStub();
    sinon.stub(client, 'getClient').returns(Promise.resolve(thriftClient));

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
