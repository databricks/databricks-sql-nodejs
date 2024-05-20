import { expect } from 'chai';
import OAuthManager from '../../../lib/connection/auth/DatabricksOAuth/OAuthManager';
import OAuthToken from '../../../lib/connection/auth/DatabricksOAuth/OAuthToken';
import { OAuthScopes, scopeDelimiter } from '../../../lib/connection/auth/DatabricksOAuth/OAuthScope';
import OAuthPersistence from '../../../lib/connection/auth/DatabricksOAuth/OAuthPersistence';
import { EventEmitter } from 'events';
import { IncomingMessage, ServerResponse, RequestListener } from 'http';
import { ListenOptions } from 'net';

export function createAccessToken(expirationTime: number) {
  const payload = Buffer.from(JSON.stringify({ exp: expirationTime }), 'utf8').toString('base64');
  return `access.${payload}`;
}

export function createValidAccessToken() {
  const expirationTime = Math.trunc(Date.now() / 1000) + 20000;
  return createAccessToken(expirationTime);
}

export function createExpiredAccessToken() {
  const expirationTime = Math.trunc(Date.now() / 1000) - 1000;
  return createAccessToken(expirationTime);
}

export class OAuthPersistenceStub implements OAuthPersistence {
  public token?: OAuthToken;

  async persist(host: string, token: OAuthToken) {
    this.token = token;
  }

  async read() {
    return this.token;
  }
}

export class OAuthCallbackServerStub<
  Request extends typeof IncomingMessage = typeof IncomingMessage,
  Response extends typeof ServerResponse = typeof ServerResponse,
> extends EventEmitter {
  public requestHandler: RequestListener<Request, Response>;

  public listening = false;

  public listenError?: Error; // error to emit on listen

  public closeError?: Error; // error to emit on close

  constructor(requestHandler?: RequestListener<Request, Response>) {
    super();
    this.requestHandler =
      requestHandler ??
      (() => {
        throw new Error('OAuthCallbackServerStub: no request handler provided');
      });
  }

  // We support only one of these signatures, but have to declare all for compatibility with `http.Server`
  listen(port?: number, hostname?: string, backlog?: number, listeningListener?: () => void): this;
  listen(port?: number, hostname?: string, listeningListener?: () => void): this;
  listen(port?: number, backlog?: number, listeningListener?: () => void): this;
  listen(port?: number, listeningListener?: () => void): this;
  listen(path: string, backlog?: number, listeningListener?: () => void): this;
  listen(path: string, listeningListener?: () => void): this;
  listen(options: ListenOptions, listeningListener?: () => void): this;
  listen(handle: any, backlog?: number, listeningListener?: () => void): this;
  listen(handle: any, listeningListener?: () => void): this;
  listen(...args: unknown[]) {
    const [port, host, callback] = args;

    if (typeof port !== 'number' || typeof host !== 'string' || typeof callback !== 'function') {
      throw new TypeError('Only this signature supported: `listen(port: number, host: string, callback: () => void)`');
    }

    if (this.listenError) {
      this.emit('error', this.listenError);
      this.listenError = undefined;
    } else if (port < 1000) {
      const error = new Error(`Address ${host}:${port} is already in use`);
      (error as any).code = 'EADDRINUSE';
      this.emit('error', error);
    } else {
      this.listening = true;
      callback();
    }

    return this;
  }

  close(callback: () => void) {
    this.requestHandler = () => {};
    this.listening = false;
    if (this.closeError) {
      this.emit('error', this.closeError);
      this.closeError = undefined;
    } else {
      callback();
    }

    return this;
  }

  // Dummy methods and properties for compatibility with `http.Server`

  public maxHeadersCount: number | null = null;

  public maxRequestsPerSocket: number | null = null;

  public timeout: number = -1;

  public headersTimeout: number = -1;

  public keepAliveTimeout: number = -1;

  public requestTimeout: number = -1;

  public maxConnections: number = -1;

  public connections: number = 0;

  public setTimeout() {
    return this;
  }

  public closeAllConnections() {}

  public closeIdleConnections() {}

  public address() {
    return null;
  }

  public getConnections() {}

  public ref() {
    return this;
  }

  public unref() {
    return this;
  }
}

export class AuthorizationCodeStub {
  public fetchResult: unknown = undefined;

  public expectedScope?: string = undefined;

  static validCode = {
    code: 'auth_code',
    verifier: 'verifier_string',
    redirectUri: 'http://localhost:8000',
  };

  async fetch(scopes: Array<string>) {
    if (this.expectedScope) {
      expect(scopes.join(scopeDelimiter)).to.be.equal(this.expectedScope);
    }
    return this.fetchResult;
  }
}

export class OAuthManagerStub extends OAuthManager {
  public getTokenResult = new OAuthToken(createValidAccessToken());

  public refreshTokenResult = new OAuthToken(createValidAccessToken());

  protected getOIDCConfigUrl(): string {
    throw new Error('Not implemented');
  }

  protected getAuthorizationUrl(): string {
    throw new Error('Not implemented');
  }

  protected getClientId(): string {
    throw new Error('Not implemented');
  }

  protected getCallbackPorts(): Array<number> {
    throw new Error('Not implemented');
  }

  protected getScopes(requestedScopes: OAuthScopes) {
    return requestedScopes;
  }

  public async refreshAccessToken(token: OAuthToken) {
    return token.hasExpired ? this.refreshTokenResult : token;
  }

  public async getToken() {
    return this.getTokenResult;
  }
}
