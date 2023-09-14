const http = require('http');
const https = require('https');
const { expect } = require('chai');
const HttpConnection = require('../../../../dist/connection/connections/HttpConnection').default;
const ThriftHttpConnection = require('../../../../dist/connection/connections/ThriftHttpConnection').default;

const thriftMock = (connection) => ({
  createHttpConnection(host, port, options) {
    this.host = host;
    this.port = port;
    this.options = options;
    this.executed = true;
    return connection;
  },
});

describe('HttpConnection.connect', () => {
  it('should successfully connect', async () => {
    const connection = new HttpConnection();

    expect(connection.isConnected()).to.be.false;

    await connection.connect({
      host: 'localhost',
      port: 10001,
      path: '/hive',
    });

    expect(connection.connection.url).to.be.eq('http://localhost:10001/hive');
    expect(connection.getConnection()).to.be.instanceOf(ThriftHttpConnection);
    expect(connection.isConnected()).to.be.true;
  });

  it('should set SSL certificates and disable rejectUnauthorized', async () => {
    const connection = new HttpConnection();

    await connection.connect({
      host: 'localhost',
      port: 10001,
      path: '/hive',
      https: true,
      ca: 'ca',
      cert: 'cert',
      key: 'key',
    });

    expect(connection.connection.config.agent.options.rejectUnauthorized).to.be.false;
    expect(connection.connection.config.agent.options.ca).to.be.eq('ca');
    expect(connection.connection.config.agent.options.cert).to.be.eq('cert');
    expect(connection.connection.config.agent.options.key).to.be.eq('key');
  });

  it('should initialize http agents', async () => {
    const connection = new HttpConnection();

    await connection.connect({
      host: 'localhost',
      port: 10001,
      https: false,
      path: '/hive',
    });

    expect(connection.connection.config.agent).to.be.instanceOf(http.Agent);
  });
});
