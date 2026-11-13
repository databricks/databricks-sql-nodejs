import thrift from 'thrift';
import https from 'https';
import http from 'http';
import { HeadersInit } from 'node-fetch';
import { ProxyAgent } from 'proxy-agent';

import IConnectionProvider, { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import IConnectionOptions, { ProxyOptions } from '../contracts/IConnectionOptions';
import IClientContext from '../../contracts/IClientContext';
import { LogLevel } from '../../contracts/IDBSQLLogger';
import HiveDriverError from '../../errors/HiveDriverError';

import ThriftHttpConnection from './ThriftHttpConnection';
import IRetryPolicy from '../contracts/IRetryPolicy';
import HttpRetryPolicy from './HttpRetryPolicy';
import UnsentRequestRetryPolicy from './UnsentRequestRetryPolicy';

// Set by the driver, auth provider or Thrift transport. node-fetch merges
// same-named headers case-insensitively into one value, so a caller copy
// would corrupt the driver's value rather than replace it.
const DRIVER_MANAGED_HEADERS = new Set([
  'authorization',
  'user-agent',
  'content-type',
  'content-length',
  'transfer-encoding',
  'connection',
]);

// node-fetch's header name/value rules; checked up front so the error names
// the header without echoing its (possibly secret) value.
const VALID_HEADER_NAME = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const INVALID_HEADER_VALUE_CHAR = /[^\t\x20-\x7e\x80-\xff]/;

function getProtocol(url: string | URL): string {
  if (typeof url !== 'string') {
    return url.protocol;
  }
  const match = /^([a-z][a-z0-9+.-]*:)/i.exec(url);
  return match ? match[1].toLowerCase() : '';
}

export default class HttpConnection implements IConnectionProvider {
  private readonly options: IConnectionOptions;

  private readonly context: IClientContext;

  private headers: HeadersInit = {};

  // Validated caller `customHeaders` without driver-managed names; built once.
  private customHeaders?: Record<string, string>;

  private connection?: ThriftHttpConnection;

  private proxyAgent?: http.Agent;

  private httpAgent?: http.Agent;

  private httpsAgent?: https.Agent;

  constructor(options: IConnectionOptions, context: IClientContext) {
    this.options = options;
    this.context = context;
  }

  public setHeaders(headers: HeadersInit) {
    this.headers = headers;
    this.connection?.setHeaders(this.getRequestHeaders());
  }

  private getRequestHeaders(): HeadersInit {
    if (!this.customHeaders) {
      this.customHeaders = this.getCustomHeaders();
    }
    if (Object.keys(this.customHeaders).length === 0) {
      return { ...this.options.headers, ...this.headers };
    }
    // Drop caller keys that `options.headers` or the auth headers also set, in
    // any casing: node-fetch would join both into one value instead of letting
    // the later one win. Re-checked on every call as auth headers change.
    const reserved = new Set(
      [...Object.keys(this.options.headers ?? {}), ...Object.keys(this.headers)].map((name) => name.toLowerCase()),
    );
    const custom: Record<string, string> = {};
    for (const [name, value] of Object.entries(this.customHeaders)) {
      if (!reserved.has(name.toLowerCase())) {
        custom[name] = value;
      }
    }
    return { ...custom, ...this.options.headers, ...this.headers };
  }

  private getCustomHeaders(): Record<string, string> {
    const result: Record<string, string> = {};
    for (const [name, value] of Object.entries(this.options.customHeaders ?? {})) {
      if (!VALID_HEADER_NAME.test(name)) {
        throw new HiveDriverError(`customHeaders name ${JSON.stringify(name)} is not a valid HTTP header name.`);
      }
      if (INVALID_HEADER_VALUE_CHAR.test(value)) {
        throw new HiveDriverError(
          `customHeaders value for \`${name}\` contains a character that is not allowed in an HTTP header ` +
            `(such as CR, LF or NUL).`,
        );
      }
      if (DRIVER_MANAGED_HEADERS.has(name.toLowerCase())) {
        this.context
          .getLogger()
          .log(
            LogLevel.warn,
            `customHeaders: \`${name}\` is managed by the driver and is not sent on Thrift requests.`,
          );
      } else {
        result[name] = value;
      }
    }
    return result;
  }

  /**
   * Returns the agent for a request to `url` (an absolute URL or a bare
   * protocol such as `'https:'`). Node rejects an agent whose protocol differs
   * from the request's, so the agent follows the URL rather than `https`, which
   * only selects the SQL transport. Without `url`, returns the SQL transport's agent.
   */
  public async getAgent(url?: string | URL): Promise<http.Agent | undefined> {
    if (this.options.proxy !== undefined) {
      // ProxyAgent handles both protocols itself.
      if (!this.proxyAgent) {
        this.proxyAgent = this.createProxyAgent(this.options.proxy);
      }
      return this.proxyAgent;
    }

    const useHttps = url === undefined ? Boolean(this.options.https) : getProtocol(url) !== 'http:';
    if (useHttps) {
      if (!this.httpsAgent) {
        this.httpsAgent = this.createHttpsAgent();
      }
      return this.httpsAgent;
    }

    if (!this.httpAgent) {
      this.httpAgent = this.createHttpAgent();
    }
    return this.httpAgent;
  }

  private getAgentDefaultOptions(): http.AgentOptions {
    const clientConfig = this.context.getConfig();

    return {
      keepAlive: true,
      keepAliveMsecs: 10000,
      maxSockets: Infinity, // no limit
      timeout: this.options.socketTimeout ?? clientConfig.socketTimeout,
    };
  }

  private createHttpAgent(): http.Agent {
    const httpAgentOptions = this.getAgentDefaultOptions();
    return new http.Agent(httpAgentOptions);
  }

  private getTlsOptions(): https.AgentOptions {
    return {
      minVersion: 'TLSv1.2',
      // Validate the server certificate by default; only skip verification when
      // the caller explicitly opts out via `rejectUnauthorized: false`.
      rejectUnauthorized: this.options.rejectUnauthorized ?? true,
      ca: this.options.ca,
      cert: this.options.cert,
      key: this.options.key,
    };
  }

  private createHttpsAgent(): https.Agent {
    const httpsAgentOptions: https.AgentOptions = {
      ...this.getAgentDefaultOptions(),
      ...this.getTlsOptions(),
    };
    return new https.Agent(httpsAgentOptions);
  }

  private createProxyAgent(proxyOptions: ProxyOptions): ProxyAgent {
    const proxyAuth = proxyOptions.auth?.username
      ? `${proxyOptions.auth.username}:${proxyOptions.auth?.password ?? ''}@`
      : '';
    const proxyUrl = `${proxyOptions.protocol}://${proxyAuth}${proxyOptions.host}:${proxyOptions.port}`;

    return new ProxyAgent({
      ...this.getAgentDefaultOptions(),
      // `httpsAgent` below only serves URLs that bypass the proxy. Tunnelled TLS
      // connections take their settings from these top-level options, which
      // proxy-agent also applies to the connection to an `https:` proxy.
      ...this.getTlsOptions(),
      getProxyForUrl: () => proxyUrl,
      httpsAgent: this.createHttpsAgent(),
      httpAgent: this.createHttpAgent(),
    });
  }

  public async getThriftConnection(): Promise<any> {
    if (!this.connection) {
      const { options } = this;
      const clientConfig = this.context.getConfig();
      const headers = this.getRequestHeaders();
      const agent = await this.getAgent();

      this.connection = new ThriftHttpConnection(
        {
          url: `${options.https ? 'https' : 'http'}://${options.host.replace(/\/$/, '')}:${options.port}${
            options.path ? (options.path.startsWith('/') ? '' : '/') + options.path.replace(/\/$/, '') : '/'
          }`,
          transport: thrift.TBufferedTransport,
          protocol: thrift.TBinaryProtocol,
          getRetryPolicy: () => this.getRetryPolicy(),
          getUnsentRequestRetryPolicy: async () => new UnsentRequestRetryPolicy(this.context),
        },
        {
          agent,
          timeout: options.socketTimeout ?? clientConfig.socketTimeout,
          headers,
        },
      );
    }

    return this.connection;
  }

  public async getRetryPolicy(): Promise<IRetryPolicy<HttpTransactionDetails>> {
    return new HttpRetryPolicy(this.context);
  }
}
