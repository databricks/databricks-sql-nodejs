import http, { IncomingMessage, Server, ServerResponse } from 'http';
import { BaseClient, CallbackParamsType, generators } from 'openid-client';
import open from 'open';
import { LogLevel } from '../../../contracts/IDBSQLLogger';
import { OAuthScopes, scopeDelimiter } from './OAuthScope';
import IClientContext from '../../../contracts/IClientContext';
import AuthenticationError from '../../../errors/AuthenticationError';

export type DefaultOpenAuthUrlFunction = (authUrl: string) => Promise<void>;

export type CustomOpenAuthUrlFunction = (
  authUrl: string,
  defaultOpenAuthUrl: DefaultOpenAuthUrlFunction,
) => Promise<void>;

export interface AuthorizationCodeOptions {
  client: BaseClient;
  ports: Array<number>;
  context: IClientContext;
  openAuthUrl?: CustomOpenAuthUrlFunction;
}

async function defaultOpenAuthUrl(authUrl: string) {
  await open(authUrl);
}

async function openAuthUrl(authUrl: string, defaultOpenUrl: DefaultOpenAuthUrlFunction) {
  return defaultOpenUrl(authUrl);
}

export interface AuthorizationCodeFetchResult {
  code: string;
  verifier: string;
  redirectUri: string;
}

export default class AuthorizationCode {
  private readonly context: IClientContext;

  private readonly client: BaseClient;

  private readonly host: string = 'localhost';

  private readonly options: AuthorizationCodeOptions;

  constructor(options: AuthorizationCodeOptions) {
    this.client = options.client;
    this.context = options.context;
    this.options = options;
  }

  public async fetch(scopes: OAuthScopes): Promise<AuthorizationCodeFetchResult> {
    const verifierString = generators.codeVerifier(32);
    const challengeString = generators.codeChallenge(verifierString);
    const state = generators.state(16);

    let receivedParams: CallbackParamsType | undefined;

    const server = await this.createServer((req, res) => {
      const params = this.client.callbackParams(req);
      if (params.state === state) {
        receivedParams = params;
        res.writeHead(200);
        res.end(this.renderCallbackResponse());
        server.stop();
      } else {
        res.writeHead(404);
        res.end();
      }
    });

    const redirectUri = `http://${server.host}:${server.port}`;
    const authUrl = this.client.authorizationUrl({
      response_type: 'code',
      response_mode: 'query',
      scope: scopes.join(scopeDelimiter),
      code_challenge: challengeString,
      code_challenge_method: 'S256',
      state,
      redirect_uri: redirectUri,
    });

    const openUrl = this.options.openAuthUrl ?? openAuthUrl;
    await openUrl(authUrl, defaultOpenAuthUrl);
    await server.stopped();

    if (!receivedParams || !receivedParams.code) {
      if (receivedParams?.error) {
        const errorMessage = `OAuth error: ${receivedParams.error} ${receivedParams.error_description}`;
        throw new AuthenticationError(errorMessage);
      }
      throw new AuthenticationError(`No path parameters were returned to the callback at ${redirectUri}`);
    }

    return { code: receivedParams.code, verifier: verifierString, redirectUri };
  }

  private async createServer(requestHandler: (req: IncomingMessage, res: ServerResponse) => void) {
    for (const port of this.options.ports) {
      const host = this.host; // eslint-disable-line prefer-destructuring
      try {
        const server = await this.startServer(host, port, requestHandler); // eslint-disable-line no-await-in-loop
        this.context.getLogger().log(LogLevel.info, `Listening for OAuth authorization callback at ${host}:${port}`);

        let resolveStopped: () => void;
        let rejectStopped: (reason?: any) => void;
        const stoppedPromise = new Promise<void>((resolve, reject) => {
          resolveStopped = resolve;
          rejectStopped = reject;
        });

        return {
          host,
          port,
          server,
          stop: () => this.stopServer(server).then(resolveStopped).catch(rejectStopped),
          stopped: () => stoppedPromise,
        };
      } catch (error) {
        // if port already in use - try another one, otherwise re-throw an exception
        if (error instanceof Error && 'code' in error && error.code === 'EADDRINUSE') {
          this.context.getLogger().log(LogLevel.debug, `Failed to start server at ${host}:${port}: ${error.code}`);
        } else {
          throw error;
        }
      }
    }

    throw new AuthenticationError('Failed to start server: all ports are in use');
  }

  protected createHttpServer(requestHandler: (req: IncomingMessage, res: ServerResponse) => void) {
    return http.createServer(requestHandler);
  }

  private async startServer(
    host: string,
    port: number,
    requestHandler: (req: IncomingMessage, res: ServerResponse) => void,
  ): Promise<Server> {
    const server = this.createHttpServer(requestHandler);

    return new Promise((resolve, reject) => {
      const errorListener = (error: Error) => {
        server.off('error', errorListener);
        reject(error);
      };

      server.on('error', errorListener);
      server.listen(port, host, () => {
        server.off('error', errorListener);
        resolve(server);
      });
    });
  }

  private async stopServer(server: Server): Promise<void> {
    if (!server.listening) {
      return;
    }

    return new Promise((resolve, reject) => {
      const errorListener = (error: Error) => {
        server.off('error', errorListener);
        reject(error);
      };

      server.on('error', errorListener);
      server.close(() => {
        server.off('error', errorListener);
        resolve();
      });
    });
  }

  private renderCallbackResponse(): string {
    const applicationName = 'Databricks Sql Connector';

    return `<html lang="en">
<head>
  <title>Close this Tab</title>
  <style>
    body {
      font-family: "Barlow", Helvetica, Arial, sans-serif;
      padding: 20px;
      background-color: #f3f3f3;
    }
  </style>
</head>
<body>
  <h1>Please close this tab.</h1>
  <p>
    The ${applicationName} received a response. You may close this tab.
  </p>
</body>
</html>`;
  }
}
