const { expect } = require('chai');
const DBSQLClient = require('../../dist/DBSQLClient').default;
const DBSQLSession = require('../../dist/DBSQLSession').default;
const {
  auth: { PlainHttpAuthentication },
  connections: { HttpConnection },
} = require('../../');

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

  it('should set nosasl authenticator by default', () => {
    const client = new DBSQLClient();
    const connectionProvider = ConnectionProviderMock();

    client.connectionProvider = connectionProvider;
    return client.connect(options).catch((error) => {
      expect(connectionProvider.auth).instanceOf(PlainHttpAuthentication);
    });
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

  it('should use http connection by default', (cb) => {
    const client = new DBSQLClient();
    client.thrift = {
      createClient() {},
    };

    client
      .connect(options)
      .then(() => {
        expect(client.connectionProvider).instanceOf(HttpConnection);
        cb();
      })
      .catch(cb);
  });
});

describe('DBSQLClient.openSession', () => {
  it('should successfully open session', () => {
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
    return client.openSession().then((session) => {
      expect(session).instanceOf(DBSQLSession);
    });
  });

  it('should throw an exception when the connection is lost', (done) => {
    const client = new DBSQLClient();
    client.connection = {
      isConnected() {
        return false;
      },
    };

    client.openSession().catch((error) => {
      expect(error.message).to.be.eq('DBSQLClient: connection is lost');
      done();
    });
  });
});

describe('DBSQLClient.getClient', () => {
  it('should throw an error if the client is not set', () => {
    const client = new DBSQLClient();
    expect(() => client.getClient()).to.throw('DBSQLClient: client is not initialized');
  });
});

describe('DBSQLClient.close', () => {
  it('should close the connection if it was initiated', (cb) => {
    const client = new DBSQLClient();
    let closed = false;
    client.connection = {
      getConnection: () => ({
        end: () => {
          closed = true;
        },
      }),
    };
    client
      .close()
      .then(() => {
        expect(closed).to.be.true;
        cb();
      })
      .catch(cb);
  });

  it('should do nothing if the connection does not exist', (cb) => {
    const client = new DBSQLClient();
    client
      .close()
      .then(() => {
        expect(true).to.be.true;
        cb();
      })
      .catch(cb);
  });

  it('should do nothing if the connection exists but cannot be finished', (cb) => {
    const client = new DBSQLClient();
    client.connection = {
      getConnection: () => ({}),
    };
    client
      .close()
      .then(() => {
        expect(true).to.be.true;
        cb();
      })
      .catch(cb);
  });
});
