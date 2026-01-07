import { HeadersInit } from 'node-fetch';
import IAuthentication from '../../contracts/IAuthentication';
import ITokenProvider from './ITokenProvider';
import IClientContext from '../../../contracts/IClientContext';
import { LogLevel } from '../../../contracts/IDBSQLLogger';

/**
 * Adapts an ITokenProvider to the IAuthentication interface used by the driver.
 * This allows token providers to be used with the existing authentication system.
 */
export default class TokenProviderAuthenticator implements IAuthentication {
  private readonly tokenProvider: ITokenProvider;

  private readonly context: IClientContext;

  private readonly headers: HeadersInit;

  /**
   * Creates a new TokenProviderAuthenticator.
   * @param tokenProvider - The token provider to use for authentication
   * @param context - The client context for logging
   * @param headers - Additional headers to include with each request
   */
  constructor(tokenProvider: ITokenProvider, context: IClientContext, headers?: HeadersInit) {
    this.tokenProvider = tokenProvider;
    this.context = context;
    this.headers = headers ?? {};
  }

  async authenticate(): Promise<HeadersInit> {
    const logger = this.context.getLogger();
    const providerName = this.tokenProvider.getName();

    logger.log(LogLevel.debug, `TokenProviderAuthenticator: getting token from ${providerName}`);

    const token = await this.tokenProvider.getToken();

    if (token.isExpired()) {
      logger.log(LogLevel.warn, `TokenProviderAuthenticator: token from ${providerName} is expired`);
    }

    return token.setAuthHeader(this.headers);
  }
}
