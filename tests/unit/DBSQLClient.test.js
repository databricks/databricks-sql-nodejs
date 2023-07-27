const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const DBSQLClient = require('../../dist/DBSQLClient').default;
const DBSQLSession = require('../../dist/DBSQLSession').default;

const PlainHttpAuthentication = require('../../dist/connection/auth/PlainHttpAuthentication').default;
const DatabricksOAuth = require('../../dist/connection/auth/DatabricksOAuth').default;
const { AWSOAuthManager, AzureOAuthManager } = require('../../dist/connection/auth/DatabricksOAuth/OAuthManager');

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

    const path = 'example/path';
    const connectionOptions = client.getConnectionOptions({ ...options, path }, {});

    expect(connectionOptions.options.path).to.equal(`/${path}`);
  });

  it('should not prepend "/" to path if it is already available', async () => {
    const client = new DBSQLClient();

    const path = '/example/path';
    const connectionOptions = client.getConnectionOptions({ ...options, path }, {});

    expect(connectionOptions.options.path).to.equal(path);
  });

  // client.connect() now does not actually attempt any network operations. for http it never did it
  // even before, but this test was not quite correct even then. it needs to be updated
  it.skip('should handle network errors', (cb) => {
    const client = new DBSQLClient();
    client.thrift = {
      createClient() {},
    };
    const connectionProvider = ConnectionProviderMock({
      on(name, handler) {
        handler(new Error('network error'));
      },
    });

    sinon.stub(client, 'createConnection').returns(Promise.resolve(connectionProvider));

    client.on('error', (error) => {
      expect(error.message).to.be.eq('network error');
      cb();
    });

    client.connectionProvider = connectionProvider;
    client.connect(options).catch((error) => {
      cb(error);
    });
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
  it('should throw an error if the client is not set', async () => {
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
});

describe('DBSQLClient.close', () => {
  it('should close the connection if it was initiated', async () => {
    const client = new DBSQLClient();
    client.authProvider = {};
    client.connectionOptions = {};

    await client.close();
    expect(client.authProvider).to.be.null;
    expect(client.connectionOptions).to.be.null;
    // No additional asserts needed - it should just reach this point
  });

  it('should do nothing if the connection does not exist', async () => {
    const client = new DBSQLClient();

    await client.close();
    expect(client.authProvider).to.be.null;
    expect(client.connectionOptions).to.be.null;
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

  it('should use Databricks OAuth method (AWS)', () => {
    const client = new DBSQLClient();

    const provider = client.getAuthProvider({
      authType: 'databricks-oauth',
      // host is used when creating OAuth manager, so make it look like a real AWS instance
      host: 'example.dev.databricks.com',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
    expect(provider.manager).to.be.instanceOf(AWSOAuthManager);
  });

  it('should use Databricks OAuth method (Azure)', () => {
    const client = new DBSQLClient();

    const provider = client.getAuthProvider({
      authType: 'databricks-oauth',
      // host is used when creating OAuth manager, so make it look like a real Azure instance
      host: 'example.databricks.azure.us',
    });

    expect(provider).to.be.instanceOf(DatabricksOAuth);
    expect(provider.manager).to.be.instanceOf(AzureOAuthManager);
  });

  it('should throw error when OAuth not supported for host', () => {
    const client = new DBSQLClient();

    expect(() => {
      client.getAuthProvider({
        authType: 'databricks-oauth',
        // use host which is not supported for sure
        host: 'example.com',
      });
    }).to.throw();
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
