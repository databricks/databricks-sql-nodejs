const { expect } = require('chai');
const sinon = require('sinon');
const DBSQLClient = require('../../dist/DBSQLClient').default;
const DBSQLSession = require('../../dist/DBSQLSession').default;

const PlainHttpAuthentication = require('../../dist/connection/auth/PlainHttpAuthentication').default;
const DatabricksOAuth = require('../../dist/connection/auth/DatabricksOAuth').default;
const HttpConnection = require('../../dist/connection/connections/HttpConnection').default;

const ConnectionProviderMock = (connection) => ({
  connect(options, auth) {
    this.options = options;
    this.auth = auth;

    return Promise.resolve({
      getConnection() {
        return (
          connection || {
            on: () => {},
          }
        );
      },
    });
  },
});

describe('DBSQLClient.connect', () => {
  const options = {
    host: '127.0.0.1',
    path: '',
    token: 'dapi********************************',
  };

  it('should prepend "/" to path if it is missing', async () => {
    const client = new DBSQLClient();
    client.thrift = {
      createClient() {},
    };
    const connectionProvider = ConnectionProviderMock();

    const path = 'example/path';

    client.connectionProvider = connectionProvider;
    await client.connect({
      ...options,
      path,
    });

    expect(connectionProvider.options.options.path).to.equal(`/${path}`);
  });

  it('should not prepend "/" to path if it is already available', async () => {
    const client = new DBSQLClient();
    client.thrift = {
      createClient() {},
    };
    const connectionProvider = ConnectionProviderMock();

    const path = '/example/path';

    client.connectionProvider = connectionProvider;
    await client.connect({
      ...options,
      path,
    });

    expect(connectionProvider.options.options.path).to.equal(path);
  });

  it('should set nosasl authenticator by default', async () => {
    const client = new DBSQLClient();
    const connectionProvider = ConnectionProviderMock();

    client.connectionProvider = connectionProvider;
    try {
      await client.connect(options);
    } catch {
      expect(connectionProvider.auth).instanceOf(PlainHttpAuthentication);
    }
  });

  it('should handle network errors', (cb) => {
    const client = new DBSQLClient();
    client.thrift = {
      createClient() {},
    };
    const connectionProvider = ConnectionProviderMock({
      on(name, handler) {
        handler(new Error('network error'));
      },
    });

    client.on('error', (error) => {
      expect(error.message).to.be.eq('network error');
      cb();
    });

    client.connectionProvider = connectionProvider;
    client.connect(options).catch((error) => {
      cb(error);
    });
  });

  it('should use http connection by default', async () => {
    const client = new DBSQLClient();
    client.thrift = {
      createClient() {},
    };

    await client.connect(options);
    expect(client.connectionProvider).instanceOf(HttpConnection);
  });
});

describe('DBSQLClient.openSession', () => {
  it('should successfully open session', async () => {
    const client = new DBSQLClient();
    client.client = {
      OpenSession(req, cb) {
        cb(null, { status: {}, sessionHandle: {} });
      },
    };
    client.connection = {
      isConnected() {
        return true;
      },
    };

    const session = await client.openSession();
    expect(session).instanceOf(DBSQLSession);
  });

  it('should use initial namespace options', async () => {
    const client = new DBSQLClient();
    client.client = {
      OpenSession(req, cb) {
        cb(null, { status: {}, sessionHandle: {} });
      },
    };
    client.connection = {
      isConnected() {
        return true;
      },
    };

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
      expect(error.message).to.be.eq('DBSQLClient: connection is lost');
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
      expect(error.message).to.be.eq('DBSQLClient: connection is lost');
    }
  });
});

describe('DBSQLClient.getClient', () => {
  it('should throw an error if the client is not set', () => {
    const client = new DBSQLClient();
    expect(() => client.getClient()).to.throw('DBSQLClient: client is not initialized');
  });
});

describe('DBSQLClient.close', () => {
  it('should close the connection if it was initiated', async () => {
    const client = new DBSQLClient();
    const closeConnectionStub = sinon.stub();
    client.connection = {
      getConnection: () => ({
        end: closeConnectionStub,
      }),
    };

    await client.close();
    expect(closeConnectionStub.called).to.be.true;
    // No additional asserts needed - it should just reach this point
  });

  it('should do nothing if the connection does not exist', async () => {
    const client = new DBSQLClient();

    await client.close();
    // No additional asserts needed - it should just reach this point
  });

  it('should do nothing if the connection exists but cannot be finished', async () => {
    const client = new DBSQLClient();
    client.connection = {
      getConnection: () => ({}),
    };

    await client.close();
    // No additional asserts needed - it should just reach this point
  });
});

describe('DBSQLClient.getAuthProvider', () => {
  it('should use access token auth method', () => {
    const client = new DBSQLClient();

    const testAccessToken = 'token';
    const provider = client.getAuthProvider({
      authType: 'access-token',
      token: testAccessToken,
    });

    expect(provider).to.be.instanceOf(PlainHttpAuthentication);
    expect(provider.password).to.be.equal(testAccessToken);
  });

  it('should use access token auth method by default (compatibility)', () => {
    const client = new DBSQLClient();

    const testAccessToken = 'token';
    const provider = client.getAuthProvider({
      // note: no `authType` provided
      token: testAccessToken,
    });

    expect(provider).to.be.instanceOf(PlainHttpAuthentication);
    expect(provider.password).to.be.equal(testAccessToken);
  });

  it('should use Databricks OAuth method', () => {
    const client = new DBSQLClient();

    const provider = client.getAuthProvider({
      authType: 'databricks-oauth',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
  });

  it('should use custom auth method', () => {
    const client = new DBSQLClient();

    const customProvider = {};

    const provider = client.getAuthProvider({
      authType: 'custom',
      provider: customProvider,
    });

    expect(provider).to.be.equal(customProvider);
  });

  it('should use custom auth method (legacy way)', () => {
    const client = new DBSQLClient();

    const customProvider = {};

    const provider = client.getAuthProvider(
      // custom provider from second arg should be used no matter what's specified in config
      { authType: 'access-token', token: 'token' },
      customProvider,
    );

    expect(provider).to.be.equal(customProvider);
  });
});
