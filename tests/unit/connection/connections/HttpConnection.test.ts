import { execFileSync } from 'child_process';
import fs from 'fs';
import http from 'http';
import https from 'https';
import net from 'net';
import os from 'os';
import path from 'path';
import tls from 'tls';
import { expect } from 'chai';
import { ProxyAgent } from 'proxy-agent';
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

  it('should set SSL certificates and validate the server certificate by default', async () => {
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

    expect(thriftConnection.config.agent.options.rejectUnauthorized).to.be.true;
    expect(thriftConnection.config.agent.options.ca).to.be.eq('ca');
    expect(thriftConnection.config.agent.options.cert).to.be.eq('cert');
    expect(thriftConnection.config.agent.options.key).to.be.eq('key');
  });

  it('should allow disabling server certificate validation via rejectUnauthorized', async () => {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
        https: true,
        rejectUnauthorized: false,
      },
      new ClientContextStub(),
    );

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection.config.agent.options.rejectUnauthorized).to.be.false;
  });

  it('should keep server certificate validation enabled when rejectUnauthorized is true', async () => {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 10001,
        path: '/hive',
        https: true,
        rejectUnauthorized: true,
      },
      new ClientContextStub(),
    );

    const thriftConnection = await connection.getThriftConnection();

    expect(thriftConnection.config.agent.options.rejectUnauthorized).to.be.true;
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

  it('should pass TLS options to the proxy agent', async () => {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: 443,
        https: true,
        proxy: { protocol: 'http', host: 'localhost', port: 8080 },
        ca: 'ca',
        cert: 'cert',
        key: 'key',
      },
      new ClientContextStub(),
    );

    const agent = (await connection.getAgent()) as ProxyAgent;

    expect(agent).to.be.instanceOf(ProxyAgent);
    expect(agent.connectOpts).to.include({
      minVersion: 'TLSv1.2',
      rejectUnauthorized: true,
      ca: 'ca',
      cert: 'cert',
      key: 'key',
    });
  });
});

// `tls.getCACertificates` / `tls.setDefaultCACertificates` exist on every supported Node line
// but are newer than the pinned @types/node.
const tlsDefaultCa = tls as unknown as {
  getCACertificates?: (type: 'default') => string[];
  setDefaultCACertificates?: (certs: string[]) => void;
};

describe('HttpConnection through an HTTP CONNECT proxy', () => {
  // Self-signed `localhost` certificate, generated per run so no private key is committed.
  let cert = '';
  let key = '';

  let server: https.Server;
  let serverRequests = 0;
  let clientCertCN: string | undefined;

  let proxy: http.Server;
  let proxyConnects = 0;
  const tunnels = new Set<net.Socket>();

  const agents: Array<http.Agent | undefined> = [];

  before(async function () {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbsql-proxy-tls-'));
    try {
      execFileSync(
        'openssl',
        [
          'req',
          '-x509',
          '-newkey',
          'ec',
          '-pkeyopt',
          'ec_paramgen_curve:prime256v1',
          '-nodes',
          '-days',
          '1',
          '-subj',
          '/CN=localhost',
          '-addext',
          'subjectAltName=DNS:localhost,IP:127.0.0.1',
          '-keyout',
          path.join(dir, 'key.pem'),
          '-out',
          path.join(dir, 'cert.pem'),
        ],
        { stdio: 'ignore' },
      );
      cert = fs.readFileSync(path.join(dir, 'cert.pem'), 'utf8');
      key = fs.readFileSync(path.join(dir, 'key.pem'), 'utf8');
    } catch {
      this.skip(); // openssl is not available
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }

    server = https.createServer({ cert, key, requestCert: true, rejectUnauthorized: false }, (req, res) => {
      serverRequests += 1;
      clientCertCN = (req.socket as tls.TLSSocket).getPeerCertificate().subject?.CN;
      req.resume();
      res.end('ok');
    });

    proxy = http.createServer();
    proxy.on('connect', (req: http.IncomingMessage, clientSocket: net.Socket, head: Buffer) => {
      proxyConnects += 1;
      const upstream = net.connect(Number(String(req.url).split(':')[1]), '127.0.0.1', () => {
        clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        upstream.write(head);
        upstream.pipe(clientSocket);
        clientSocket.pipe(upstream);
      });
      tunnels.add(clientSocket).add(upstream);
      upstream.on('error', () => clientSocket.destroy());
      clientSocket.on('error', () => upstream.destroy());
    });

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    await new Promise<void>((resolve) => {
      proxy.listen(0, '127.0.0.1', resolve);
    });
  });

  beforeEach(() => {
    serverRequests = 0;
    clientCertCN = undefined;
    proxyConnects = 0;
  });

  afterEach(() => {
    agents.splice(0).forEach((agent) => agent?.destroy());
  });

  after(async () => {
    tunnels.forEach((socket) => socket.destroy());
    server?.closeAllConnections();
    await Promise.all(
      [server, proxy].map(
        (s) =>
          new Promise<void>((resolve) => {
            if (!s?.listening) {
              resolve();
              return;
            }
            s.close(() => resolve());
          }),
      ),
    );
  });

  async function post(options: Partial<IConnectionOptions>): Promise<void> {
    const connection = new HttpConnection(
      {
        host: 'localhost',
        port: (server.address() as net.AddressInfo).port,
        https: true,
        proxy: { protocol: 'http', host: '127.0.0.1', port: (proxy.address() as net.AddressInfo).port },
        ...options,
      },
      new ClientContextStub(),
    );
    const agent = await connection.getAgent();
    agents.push(agent);

    await new Promise<void>((resolve, reject) => {
      const req = https.request(
        {
          host: 'localhost',
          port: (server.address() as net.AddressInfo).port,
          method: 'POST',
          path: '/sql/1.0/warehouses/test',
          headers: { Authorization: 'Bearer test-token' },
          agent,
        },
        (res) => {
          res.resume();
          res.on('end', resolve);
        },
      );
      req.on('error', reject);
      req.end();
    });
  }

  async function expectRejected(options: Partial<IConnectionOptions>): Promise<NodeJS.ErrnoException> {
    let error: NodeJS.ErrnoException | undefined;
    try {
      await post(options);
    } catch (e) {
      error = e as NodeJS.ErrnoException;
    }
    expect(error, 'expected the TLS handshake to fail').to.not.be.undefined;
    expect(proxyConnects).to.equal(1);
    // The handshake fails before the request (and its bearer token) is written.
    expect(serverRequests).to.equal(0);
    return error as NodeJS.ErrnoException;
  }

  it('should verify the server certificate by default', async () => {
    const error = await expectRejected({});
    expect(error.code).to.equal('DEPTH_ZERO_SELF_SIGNED_CERT');
  });

  it('should verify against the configured CA set rather than the process default trust store', async function () {
    if (!tlsDefaultCa.getCACertificates || !tlsDefaultCa.setDefaultCACertificates) {
      this.skip();
    }
    // Trust the server's certificate process-wide (as NODE_EXTRA_CA_CERTS would), while the
    // connection's own CA set (the built-in roots) does not include it.
    const defaults = tlsDefaultCa.getCACertificates('default');
    tlsDefaultCa.setDefaultCACertificates([...defaults, cert]);
    try {
      const error = await expectRejected({ ca: [...tls.rootCertificates] });
      expect(error.code).to.equal('DEPTH_ZERO_SELF_SIGNED_CERT');
    } finally {
      tlsDefaultCa.setDefaultCACertificates(defaults);
    }
  });

  it('should allow disabling server certificate validation via rejectUnauthorized', async () => {
    await post({ rejectUnauthorized: false });
    expect(proxyConnects).to.equal(1);
    expect(serverRequests).to.equal(1);
  });

  it('should apply the CA and the client certificate', async () => {
    await post({ ca: cert, cert, key });
    expect(proxyConnects).to.equal(1);
    expect(serverRequests).to.equal(1);
    expect(clientCertCN).to.equal('localhost');
  });
});
