import http from 'http';
import { expect } from 'chai';
import HttpConnection from '../../../../lib/connection/connections/HttpConnection';
import ThriftHttpConnection from '../../../../lib/connection/connections/ThriftHttpConnection';
import IConnectionOptions from '../../../../lib/connection/contracts/IConnectionOptions';
import ClientContextStub from '../../.stubs/ClientContextStub';

describe('HttpConnection.connect', () => {
  it('should create Thrift connection', async () => {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
      },
      new ClientContextStub(),
    );

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection).to.be.instanceOf(ThriftHttpConnection);
    expect(thriftConnection.url).to.be.equal('http://localhost:10001/hive');

    // We expect that connection will be cached
    const anotherConnection = await connection.getThriftConnection();
    expect(anotherConnection).to.eq(thriftConnection);
  });

  it('should set SSL certificates and disable rejectUnauthorized', async () => {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
        https: true,
        ca: 'ca',
        cert: 'cert',
        key: 'key',
      },
      new ClientContextStub(),
    );

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection.config.agent.options.rejectUnauthorized).to.be.false;
    expect(thriftConnection.config.agent.options.ca).to.be.eq('ca');
    expect(thriftConnection.config.agent.options.cert).to.be.eq('cert');
    expect(thriftConnection.config.agent.options.key).to.be.eq('key');
  });

  it('should initialize http agents', async () => {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        https: false,
        path: '/hive',
      },
      new ClientContextStub(),
    );

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection.config.agent).to.be.instanceOf(http.Agent);
  });

  it('should update headers (case 1: Thrift connection not initialized)', async () => {
    const initialHeaders = {
      a: 'test header A',
      b: 'test header B',
    };

    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
        headers: initialHeaders,
      },
      new ClientContextStub(),
    );

    const extraHeaders = {
      b: 'new header B',
      c: 'test header C',
    };
    connection.setHeaders(extraHeaders);
    expect(connection['headers']).to.deep.equal(extraHeaders);

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

    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
        headers: initialHeaders,
      },
      new ClientContextStub(),
    );

    const thriftConnection = await connection.getThriftConnection();

    expect(connection['headers']).to.deep.equal({});
    expect(thriftConnection.config.headers).to.deep.equal(initialHeaders);

    const extraHeaders = {
      b: 'new header B',
      c: 'test header C',
    };
    connection.setHeaders(extraHeaders);
    expect(connection['headers']).to.deep.equal(extraHeaders);
    expect(thriftConnection.config.headers).to.deep.equal({
      ...initialHeaders,
      ...extraHeaders,
    });
  });

  it('should handle trailing slashes in host correctly', async () => {
    interface TestCase {
      input: {
        host: string;
        path?: string;
      };
      expected: string;
    }

    const testCases: TestCase[] = [
      {
        input: { host: 'xyz.com/', path: '/sql/v1' },
        expected: 'https://xyz.com:443/sql/v1',
      },
      {
        input: { host: 'xyz.com', path: '/sql/v1' },
        expected: 'https://xyz.com:443/sql/v1',
      },
      {
        input: { host: 'xyz.com/', path: undefined },
        expected: 'https://xyz.com:443/',
      },
      {
        input: { host: 'xyz.com', path: 'sql/v1' },
        expected: 'https://xyz.com:443/sql/v1',
      },
      {
        input: { host: 'xyz.com/', path: 'sql/v1' },
        expected: 'https://xyz.com:443/sql/v1',
      },
      {
        input: { host: 'xyz.com', path: 'sql/v1/' },
        expected: 'https://xyz.com:443/sql/v1',
      },
      {
        input: { host: 'https://xyz.com', path: 'sql/v1' },
        expected: 'https://xyz.com:443/sql/v1',
      },
    ];

    for (const testCase of testCases) {
      const options: IConnectionOptions = {
        host: testCase.input.host,
        port: 443,
        path: testCase.input.path,
        https: true,
      };

      const connection = new HttpConnection(options, new ClientContextStub());
      const thriftConnection = await connection.getThriftConnection();
      expect(thriftConnection.url).to.be.equal(testCase.expected);
    }
  });
});
