const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const DBSQLClient = require('../../dist/DBSQLClient').default;
const DBSQLSession = require('../../dist/DBSQLSession').default;

const PlainHttpAuthentication = require('../../dist/connection/auth/PlainHttpAuthentication').default;
const DatabricksOAuth = require('../../dist/connection/auth/DatabricksOAuth').default;
const {
  DatabricksOAuthManager,
  AzureOAuthManager,
} = require('../../dist/connection/auth/DatabricksOAuth/OAuthManager');

const HttpConnectionModule = require('../../dist/connection/connections/HttpConnection');

const { default: HttpConnection } = HttpConnectionModule;

class AuthProviderMock {
  constructor() {
    this.authResult = {};
  }

  authenticate() {
    return Promise.resolve(this.authResult);
  }
}

describe('DBSQLClient.connect', () => {
  const options = {
    host: '127.0.0.1',
    path: '',
    token: 'dapi********************************',
  };

  afterEach(() => {
    HttpConnectionModule.default.restore?.();
  });

  it('should prepend "/" to path if it is missing', async () => {
    const client = new DBSQLClient();

    const path = 'example/path';
    const connectionOptions = client.getConnectionOptions({ ...options, path }, {});

    expect(connectionOptions.path).to.equal(`/${path}`);
  });

  it('should not prepend "/" to path if it is already available', async () => {
    const client = new DBSQLClient();

    const path = '/example/path';
    const connectionOptions = client.getConnectionOptions({ ...options, path }, {});

    expect(connectionOptions.path).to.equal(path);
  });

  it('should initialize connection state', async () => {
    const client = new DBSQLClient();

    expect(client.client).to.be.undefined;
    expect(client.authProvider).to.be.undefined;
    expect(client.connectionProvider).to.be.undefined;

    await client.connect(options);

    expect(client.client).to.be.undefined; // it should not be initialized at this point
    expect(client.authProvider).to.be.instanceOf(PlainHttpAuthentication);
    expect(client.connectionProvider).to.be.instanceOf(HttpConnection);
  });

  it('should listen for Thrift connection events', async () => {
    const client = new DBSQLClient();

    const thriftConnectionMock = {
      on: sinon.stub(),
    };

    sinon.stub(HttpConnectionModule, 'default').returns({
      getThriftConnection: () => Promise.resolve(thriftConnectionMock),
    });

    await client.connect(options);

    expect(thriftConnectionMock.on.called).to.be.true;
  });
});

describe('DBSQLClient.openSession', () => {
  it('should successfully open session', async () => {
    const client = new DBSQLClient();

    sinon.stub(client, 'getClient').returns(
      Promise.resolve({
        OpenSession(req, cb) {
          cb(null, { status: {}, sessionHandle: {} });
        },
      }),
    );

    client.authProvider = {};
    client.connectionOptions = {};

    const session = await client.openSession();
    expect(session).instanceOf(DBSQLSession);
  });

  it('should use initial namespace options', async () => {
    const client = new DBSQLClient();

    sinon.stub(client, 'getClient').returns(
      Promise.resolve({
        OpenSession(req, cb) {
          cb(null, { status: {}, sessionHandle: {} });
        },
      }),
    );

    client.authProvider = {};
    client.connectionOptions = {};

    case1: {
      const session = await client.openSession({ initialCatalog: 'catalog' });
      expect(session).instanceOf(DBSQLSession);
    }

    case2: {
      const session = await client.openSession({ initialSchema: 'schema' });
      expect(session).instanceOf(DBSQLSession);
    }

    case3: {
      const session = await client.openSession({ initialCatalog: 'catalog', initialSchema: 'schema' });
      expect(session).instanceOf(DBSQLSession);
    }
  });

  it('should throw an exception when not connected', async () => {
    const client = new DBSQLClient();
    client.connection = null;

    try {
      await client.openSession();
      expect.fail('It should throw an error');
    } catch (error) {
      expect(error.message).to.be.eq('DBSQLClient: not connected');
    }
  });

  it('should throw an exception when the connection is lost', async () => {
    const client = new DBSQLClient();
    client.connection = {
      isConnected() {
        return false;
      },
    };

    try {
      await client.openSession();
      expect.fail('It should throw an error');
    } catch (error) {
      expect(error.message).to.be.eq('DBSQLClient: not connected');
    }
  });
});

describe('DBSQLClient.getClient', () => {
  const options = {
    host: '127.0.0.1',
    path: '',
    token: 'dapi********************************',
  };

  it('should throw an error if not connected', async () => {
    const client = new DBSQLClient();
    try {
      await client.getClient();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.contain('DBSQLClient: not connected');
    }
  });

  it("should create client if wasn't not initialized yet", async () => {
    const client = new DBSQLClient();

    const thriftClient = {};

    client.authProvider = new AuthProviderMock();
    client.connectionProvider = new HttpConnection({ ...options }, client);
    client.thrift = {
      createClient: sinon.stub().returns(thriftClient),
    };

    const result = await client.getClient();
    expect(client.thrift.createClient.called).to.be.true;
    expect(result).to.be.equal(thriftClient);
  });

  it('should update auth credentials each time when client is requested', async () => {
    const client = new DBSQLClient();

    const thriftClient = {};

    client.connectionProvider = new HttpConnection({ ...options }, client);
    client.thrift = {
      createClient: sinon.stub().returns(thriftClient),
    };

    sinon.stub(client.connectionProvider, 'setHeaders').callThrough();

    // just a sanity check - authProvider should be initialized by this time, but if not it should not be used
    expect(client.connectionProvider.setHeaders.callCount).to.be.equal(0);
    await client.getClient();
    expect(client.connectionProvider.setHeaders.callCount).to.be.equal(0);

    client.authProvider = new AuthProviderMock();

    // initialize client
    firstCall: {
      const result = await client.getClient();
      expect(client.thrift.createClient.callCount).to.be.equal(1);
      expect(client.connectionProvider.setHeaders.callCount).to.be.equal(1);
      expect(result).to.be.equal(thriftClient);
    }

    // credentials stay the same, client should not be re-created
    secondCall: {
      const result = await client.getClient();
      expect(client.thrift.createClient.callCount).to.be.equal(1);
      expect(client.connectionProvider.setHeaders.callCount).to.be.equal(2);
      expect(result).to.be.equal(thriftClient);
    }

    // change credentials mock - client should be re-created
    thirdCall: {
      client.authProvider.authResult = { b: 2 };

      const result = await client.getClient();
      expect(client.thrift.createClient.callCount).to.be.equal(1);
      expect(client.connectionProvider.setHeaders.callCount).to.be.equal(3);
      expect(result).to.be.equal(thriftClient);
    }
  });
});

describe('DBSQLClient.close', () => {
  it('should close the connection if it was initiated', async () => {
    const client = new DBSQLClient();
    client.client = {};
    client.connectionProvider = {};
    client.authProvider = {};

    await client.close();
    expect(client.client).to.be.undefined;
    expect(client.connectionProvider).to.be.undefined;
    expect(client.authProvider).to.be.undefined;
    // No additional asserts needed - it should just reach this point
  });

  it('should do nothing if the connection does not exist', async () => {
    const client = new DBSQLClient();

    await client.close();
    expect(client.client).to.be.undefined;
    expect(client.connectionProvider).to.be.undefined;
    expect(client.authProvider).to.be.undefined;
    // No additional asserts needed - it should just reach this point
  });

  it('should close sessions that belong to it', async () => {
    const client = new DBSQLClient();

    const thriftClientMock = {
      OpenSession(req, cb) {
        cb(null, {
          status: {},
          sessionHandle: {
            sessionId: {
              guid: Buffer.alloc(16),
              secret: Buffer.alloc(0),
            },
          },
        });
      },
      CloseSession(req, cb) {
        cb(null, { status: {} });
      },
    };
    client.client = thriftClientMock;
    sinon.stub(client, 'getClient').returns(Promise.resolve(thriftClientMock));

    const session = await client.openSession();
    expect(session.onClose).to.be.not.undefined;
    expect(session.isOpen).to.be.true;
    expect(client.sessions.items.size).to.eq(1);

    sinon.spy(thriftClientMock, 'CloseSession');
    sinon.spy(client.sessions, 'closeAll');
    sinon.spy(session, 'close');

    await client.close();
    expect(client.sessions.closeAll.called).to.be.true;
    expect(session.close.called).to.be.true;
    expect(session.onClose).to.be.undefined;
    expect(session.isOpen).to.be.false;
    expect(client.sessions.items.size).to.eq(0);
    expect(thriftClientMock.CloseSession.called).to.be.true;
  });
});

describe('DBSQLClient.initAuthProvider', () => {
  it('should use access token auth method', () => {
    const client = new DBSQLClient();

    const testAccessToken = 'token';
    const provider = client.initAuthProvider({
      authType: 'access-token',
      token: testAccessToken,
    });

    expect(provider).to.be.instanceOf(PlainHttpAuthentication);
    expect(provider.password).to.be.equal(testAccessToken);
  });

  it('should use access token auth method by default (compatibility)', () => {
    const client = new DBSQLClient();

    const testAccessToken = 'token';
    const provider = client.initAuthProvider({
      // note: no `authType` provided
      token: testAccessToken,
    });

    expect(provider).to.be.instanceOf(PlainHttpAuthentication);
    expect(provider.password).to.be.equal(testAccessToken);
  });

  it('should use Databricks OAuth method (AWS)', () => {
    const client = new DBSQLClient();

    const provider = client.initAuthProvider({
      authType: 'databricks-oauth',
      // host is used when creating OAuth manager, so make it look like a real AWS instance
      host: 'example.dev.databricks.com',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
    expect(provider.manager).to.be.instanceOf(DatabricksOAuthManager);
  });

  it('should use Databricks OAuth method (Azure)', () => {
    const client = new DBSQLClient();

    const provider = client.initAuthProvider({
      authType: 'databricks-oauth',
      // host is used when creating OAuth manager, so make it look like a real Azure instance
      host: 'example.databricks.azure.us',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
    expect(provider.manager).to.be.instanceOf(AzureOAuthManager);
  });

  it('should use Databricks InHouse OAuth method (Azure)', () => {
    const client = new DBSQLClient();

    // When `useDatabricksOAuthInAzure = true`, it should use Databricks OAuth method
    // only for supported Azure hosts, and fail for others

    case1: {
      const provider = client.initAuthProvider({
        authType: 'databricks-oauth',
        // host is used when creating OAuth manager, so make it look like a real Azure instance
        host: 'example.azuredatabricks.net',
        useDatabricksOAuthInAzure: true,
      });

      expect(provider).to.be.instanceOf(DatabricksOAuth);
      expect(provider.manager).to.be.instanceOf(DatabricksOAuthManager);
    }

    case2: {
      expect(() => {
        const provider = client.initAuthProvider({
          authType: 'databricks-oauth',
          // host is used when creating OAuth manager, so make it look like a real Azure instance
          host: 'example.databricks.azure.us',
          useDatabricksOAuthInAzure: true,
        });
      }).to.throw();
    }
  });

  it('should throw error when OAuth not supported for host', () => {
    const client = new DBSQLClient();

    expect(() => {
      client.initAuthProvider({
        authType: 'databricks-oauth',
        // use host which is not supported for sure
        host: 'example.com',
      });
    }).to.throw();
  });

  it('should use custom auth method', () => {
    const client = new DBSQLClient();

    const customProvider = {};

    const provider = client.initAuthProvider({
      authType: 'custom',
      provider: customProvider,
    });

    expect(provider).to.be.equal(customProvider);
  });

  it('should use custom auth method (legacy way)', () => {
    const client = new DBSQLClient();

    const customProvider = {};

    const provider = client.initAuthProvider(
      // custom provider from second arg should be used no matter what's specified in config
      { authType: 'access-token', token: 'token' },
      customProvider,
    );

    expect(provider).to.be.equal(customProvider);
  });
});
