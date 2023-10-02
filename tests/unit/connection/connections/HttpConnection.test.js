const http = require('http');
const { expect } = require('chai');
const HttpConnection = require('../../../../dist/connection/connections/HttpConnection').default;
const ThriftHttpConnection = require('../../../../dist/connection/connections/ThriftHttpConnection').default;

describe('HttpConnection.connect', () => {
  it('should create Thrift connection', async () => {
    const connection = new HttpConnection({
      host: 'localhost',
      port: 10001,
      path: '/hive',
    });

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection).to.be.instanceOf(ThriftHttpConnection);
    expect(thriftConnection.url).to.be.equal('http://localhost:10001/hive');

    // We expect that connection will be cached
    const anotherConnection = await connection.getThriftConnection();
    expect(anotherConnection).to.eq(thriftConnection);
  });

  it('should set SSL certificates and disable rejectUnauthorized', async () => {
    const connection = new HttpConnection({
      host: 'localhost',
      port: 10001,
      path: '/hive',
      https: true,
      ca: 'ca',
      cert: 'cert',
      key: 'key',
    });

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection.config.agent.options.rejectUnauthorized).to.be.false;
    expect(thriftConnection.config.agent.options.ca).to.be.eq('ca');
    expect(thriftConnection.config.agent.options.cert).to.be.eq('cert');
    expect(thriftConnection.config.agent.options.key).to.be.eq('key');
  });

  it('should initialize http agents', async () => {
    const connection = new HttpConnection({
      host: 'localhost',
      port: 10001,
      https: false,
      path: '/hive',
    });

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection.config.agent).to.be.instanceOf(http.Agent);
  });

  it('should update headers (case 1: Thrift connection not initialized)', async () => {
    const initialHeaders = {
      a: 'test header A',
      b: 'test header B',
    };

    const connection = new HttpConnection({
      host: 'localhost',
      port: 10001,
      path: '/hive',
      headers: initialHeaders,
    });

    const extraHeaders = {
      b: 'new header B',
      c: 'test header C',
    };
    connection.setHeaders(extraHeaders);
    expect(connection.headers).to.deep.equal(extraHeaders);

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection.config.headers).to.deep.equal({
      ...initialHeaders,
      ...extraHeaders,
    });
  });

  it('should update headers (case 2: Thrift connection initialized)', async () => {
    const initialHeaders = {
      a: 'test header A',
      b: 'test header B',
    };

    const connection = new HttpConnection({
      host: 'localhost',
      port: 10001,
      path: '/hive',
      headers: initialHeaders,
    });

    const thriftConnection = await connection.getThriftConnection();

    expect(connection.headers).to.deep.equal({});
    expect(thriftConnection.config.headers).to.deep.equal(initialHeaders);

    const extraHeaders = {
      b: 'new header B',
      c: 'test header C',
    };
    connection.setHeaders(extraHeaders);
    expect(connection.headers).to.deep.equal(extraHeaders);
    expect(thriftConnection.config.headers).to.deep.equal({
      ...initialHeaders,
      ...extraHeaders,
    });
  });
});
