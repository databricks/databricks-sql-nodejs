import http, { Server, IncomingMessage, ServerResponse } from 'http';
import { BaseClient, generators } from 'openid-client';
import open from 'open';
import IDBSQLLogger, { LogLevel } from '../../../contracts/IDBSQLLogger';

export interface AuthorizationCodeOptions {
  client: BaseClient;
  ports: Array<number>;
  logger?: IDBSQLLogger;
}

const scopeDelimiter = ' ';

async function startServer(
  host: string,
  port: number,
  requestHandler: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<Server> {
  const server = http.createServer(requestHandler);

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

async function stopServer(server: Server): Promise<void> {
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

export interface AuthorizationCodeFetchResult {
  code: string;
  verifier: string;
  redirectUri: string;
}

export default class AuthorizationCode {
  private readonly client: BaseClient;

  private readonly host: string = 'localhost';

  private readonly ports: Array<number>;

  private readonly logger?: IDBSQLLogger;

  constructor(options: AuthorizationCodeOptions) {
    this.client = options.client;
    this.ports = options.ports;
    this.logger = options.logger;
  }

  public async fetch(scopes: Array<string>): Promise<AuthorizationCodeFetchResult> {
    const verifierString = generators.codeVerifier(32);
    const challengeString = generators.codeChallenge(verifierString);
    const state = generators.state(16);

    let code: string | undefined;

    const server = await this.startServer((req, res) => {
      const params = this.client.callbackParams(req);
      if (params.state === state) {
        code = params.code;
        res.writeHead(200);
        res.end(this.renderCallbackResponse());
        server.stop();
      } else {
        res.writeHead(404);
        res.end();
      }
    });

    const redirectUri = `http://${server.host}:${server.port}/`;
    const authUrl = this.client.authorizationUrl({
      response_type: 'code',
      response_mode: 'query',
      scope: scopes.join(scopeDelimiter),
      code_challenge: challengeString,
      code_challenge_method: 'S256',
      state,
      redirect_uri: redirectUri,
    });

    await open(authUrl);
    await server.stopped();

    if (!code) {
      throw new Error(`No path parameters were returned to the callback at ${redirectUri}`);
    }

    return { code, verifier: verifierString, redirectUri };
  }

  private async startServer(requestHandler: (req: IncomingMessage, res: ServerResponse) => void) {
    for (const port of this.ports) {
      const host = this.host; // eslint-disable-line prefer-destructuring
      try {
        const server = await startServer(host, port, requestHandler); // eslint-disable-line no-await-in-loop
        this.logger?.log(LogLevel.info, `Listening for OAuth authorization callback at ${host}:${port}`);

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
          stop: () => stopServer(server).then(resolveStopped).catch(rejectStopped),
          stopped: () => stoppedPromise,
        };
      } catch (error) {
        // if port already in use - try another one, otherwise re-throw an exception
        if (error instanceof Error && 'code' in error && error.code === 'EADDRINUSE') {
          this.logger?.log(LogLevel.debug, `Failed to start server at ${host}:${port}: ${error.code}`);
        } else {
          throw error;
        }
      }
    }

    throw new Error('Failed to start server: all ports are in use');
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
